import { createClient, RedisClientType } from 'redis';
import logger from '../utils/logger';

let redisClient: RedisClientType | null = null;
let lastRedisUrl: string | null = null;

const MAX_RECONNECT_ATTEMPTS = 10;
const MAX_RECONNECT_DELAY_MS = 2000;

export async function initializeRedis(url: string): Promise<RedisClientType> {
  if (redisClient) {
    return redisClient;
  }

  try {
    redisClient = createClient({
      url: url,
      socket: {
        reconnectStrategy: (retries: number) => {
          if (retries > MAX_RECONNECT_ATTEMPTS) {
            logger.error('Max reconnection attempts reached');
            return new Error('Redis reconnection failed');
          }
          // Exponential backoff with jitter, capped at MAX_RECONNECT_DELAY_MS.
          const delay = Math.min(2 ** retries * 50, MAX_RECONNECT_DELAY_MS);
          const jitter = Math.floor(Math.random() * 100);
          return delay + jitter;
        },
      },
    });

    redisClient.on('error', (err) => logger.error('Redis error:', err));
    redisClient.on('connect', () => logger.info('Connected to Redis'));
    redisClient.on('reconnecting', () => logger.warn('Reconnecting to Redis...'));

    await redisClient.connect();
    lastRedisUrl = url;
    return redisClient;
  } catch (error) {
    logger.error('Failed to initialize Redis:', error);
    throw error;
  }
}

export function getRedisClient(): RedisClientType {
  if (!redisClient) {
    throw new Error('Redis client not initialized. Call initializeRedis first.');
  }
  return redisClient;
}

export async function closeRedis(): Promise<void> {
  if (redisClient) {
    await redisClient.quit();
    redisClient = null;
  }
}

export function isRedisConnected(): boolean {
  return redisClient?.isOpen ?? false;
}

/**
 * Report Redis connectivity for health checks.
 */
export function getRedisStatus(): { connected: boolean; url: string | null } {
  return {
    connected: isRedisConnected(),
    url: lastRedisUrl,
  };
}

/**
 * Run a Redis operation with bounded retries and exponential backoff.
 *
 * Transient failures (e.g. a brief disconnect) are retried up to `maxRetries`
 * times with an exponentially increasing delay, so callers don't fail on the
 * first blip. The original error is rethrown once the retry budget is exhausted.
 */
export async function withRedisRetry<T>(
  operation: () => Promise<T>,
  maxRetries = 3,
  baseDelayMs = 100
): Promise<T> {
  let lastError: unknown;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (attempt === maxRetries) {
        break;
      }
      const delay = baseDelayMs * 2 ** attempt;
      logger.warn({ attempt: attempt + 1, delay }, 'Redis operation failed, retrying');
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  throw lastError;
}

export interface PoolStats {
  total: number;
  inUse: number;
  idle: number;
  waiting: number;
  max: number;
  utilization: number; // 0-1, inUse / max
}

/**
 * A simple Redis connection pool with configurable min/max size.
 *
 * Connections are created lazily up to `max`. When all connections are in use,
 * `acquire()` requests are queued and resolved as connections are released, so
 * callers never exceed the connection limit under load. Use `withConnection()`
 * to run an operation with automatic acquire/release.
 */
export class RedisConnectionPool {
  private idle: RedisClientType[] = [];
  private inUse = new Set<RedisClientType>();
  private waiters: Array<(client: RedisClientType) => void> = [];
  private total = 0;

  constructor(
    private readonly factory: () => Promise<RedisClientType>,
    private readonly min = 2,
    private readonly max = 10
  ) {
    if (min < 0 || max < 1 || min > max) {
      throw new Error('Invalid pool size: require 0 <= min <= max and max >= 1');
    }
  }

  /** Pre-create the minimum number of connections. */
  async warmUp(): Promise<void> {
    while (this.total < this.min) {
      const client = await this.factory();
      this.total++;
      this.idle.push(client);
    }
  }

  /** Acquire a connection, creating one if under `max`, otherwise queueing. */
  async acquire(): Promise<RedisClientType> {
    const existing = this.idle.pop();
    if (existing) {
      this.inUse.add(existing);
      return existing;
    }

    if (this.total < this.max) {
      this.total++;
      const client = await this.factory();
      this.inUse.add(client);
      return client;
    }

    return new Promise<RedisClientType>((resolve) => {
      this.waiters.push((client) => {
        this.inUse.add(client);
        resolve(client);
      });
    });
  }

  /** Return a connection to the pool, handing it to any waiting caller. */
  release(client: RedisClientType): void {
    this.inUse.delete(client);
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter(client);
    } else {
      this.idle.push(client);
    }
  }

  /** Run an operation with a pooled connection, releasing it afterwards. */
  async withConnection<T>(fn: (client: RedisClientType) => Promise<T>): Promise<T> {
    const client = await this.acquire();
    try {
      return await fn(client);
    } finally {
      this.release(client);
    }
  }

  /** Current pool utilization for monitoring. */
  getStats(): PoolStats {
    return {
      total: this.total,
      inUse: this.inUse.size,
      idle: this.idle.length,
      waiting: this.waiters.length,
      max: this.max,
      utilization: this.max > 0 ? this.inUse.size / this.max : 0,
    };
  }

  /** Close all connections in the pool. */
  async drain(): Promise<void> {
    const all = [...this.idle, ...this.inUse];
    this.idle = [];
    this.inUse.clear();
    this.total = 0;
    await Promise.all(all.map((c) => c.quit().catch(() => undefined)));
  }
}
