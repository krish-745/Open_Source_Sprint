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
