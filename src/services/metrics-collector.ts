import { getRedisClient } from './redis';
import logger from '../utils/logger';
import { TaskQueue } from './task-queue';
import { WorkerPool } from './worker-pool';

const METRICS_PREFIX = 'metrics:';
const SNAPSHOT_PREFIX = 'snapshot:';

export interface SystemMetrics {
  timestamp: Date;
  queues: Record<string, any>;
  workers: Record<string, any>;
  tasks: Record<string, any>;
  system: Record<string, any>;
}

export class MetricsCollector {
  private static collectionRunning = false;

  // Maximum number of snapshots to retain. Older snapshots are pruned after
  // each capture so the snapshot set stays a bounded sliding window rather than
  // growing until the 7-day TTL expires.
  private static maxSnapshots = 1000;

  /**
   * Configure how many snapshots to keep (sliding-window retention).
   */
  static setMaxSnapshots(max: number): void {
    if (max > 0) {
      this.maxSnapshots = max;
    }
  }

  /**
   * Start collecting metrics periodically
   */
  static async startMetricsCollection(intervalMs: number = 60000): Promise<void> {
    if (this.collectionRunning) {
      logger.warn('Metrics collection already running');
      return;
    }

    this.collectionRunning = true;
    logger.info({ intervalMs }, 'Metrics collection started');

    const collectMetrics = async () => {
      if (!this.collectionRunning) return;

      try {
        await this.captureSnapshot();
      } catch (error) {
        logger.error({ error }, 'Metrics collection error');
      }

      setTimeout(collectMetrics, intervalMs);
    };

    collectMetrics();
  }

  /**
   * Stop metrics collection
   */
  static stopMetricsCollection(): void {
    this.collectionRunning = false;
    logger.info('Metrics collection stopped');
  }

  /**
   * Capture a full system snapshot
   */
  static async captureSnapshot(): Promise<SystemMetrics> {
    const client = getRedisClient();
    const timestamp = new Date();

    const metrics: SystemMetrics = {
      timestamp,
      queues: {},
      workers: {},
      tasks: {},
      system: {
        memoryUsage: process.memoryUsage(),
        uptime: process.uptime(),
      },
    };

    // Collect queue metrics
    const queueKeys = await client.keys('queue:*:stats');
    for (const key of queueKeys) {
      const queueName = key.replace('queue:', '').replace(':stats', '');
      const stats = await TaskQueue.getQueueStats(queueName);
      metrics.queues[queueName] = stats;
    }

    // Collect worker metrics
    const workerIds = await client.zRange('workers:index', 0, -1);
    for (const workerId of workerIds) {
      try {
        const workerMetrics = await WorkerPool.getWorkerMetrics(workerId);
        metrics.workers[workerId] = workerMetrics;
      } catch (error) {
        // Worker may not exist
      }
    }

    // Collect task stats
    const taskIndexSize = await client.zCard('tasks:index');
    const dlqSize = await client.lLen('dlq:tasks');

    metrics.tasks = {
      totalInSystem: taskIndexSize,
      deadLetterQueueSize: dlqSize,
    };

    // Store snapshot
    const snapshotKey = `${SNAPSHOT_PREFIX}${Date.now()}`;
    await client.set(snapshotKey, JSON.stringify(metrics), { EX: 7 * 24 * 60 * 60 });

    // Enforce the retention window so snapshots don't accumulate unbounded.
    await this._pruneOldSnapshots();

    logger.debug({ timestamp }, 'Metrics snapshot captured');

    return metrics;
  }

  /**
   * Delete snapshots beyond the configured retention limit, keeping the newest
   * `maxSnapshots` by numeric timestamp.
   */
  private static async _pruneOldSnapshots(): Promise<void> {
    const client = getRedisClient();
    const keys = await client.keys(`${SNAPSHOT_PREFIX}*`);

    if (keys.length <= this.maxSnapshots) {
      return;
    }

    const sortedNewestFirst = keys.sort(
      (a, b) => this._extractSnapshotTimestamp(b) - this._extractSnapshotTimestamp(a)
    );
    const toDelete = sortedNewestFirst.slice(this.maxSnapshots);

    for (const key of toDelete) {
      await client.del(key);
    }

    logger.debug({ pruned: toDelete.length, retained: this.maxSnapshots }, 'Old metric snapshots pruned');
  }

  /**
   * Get latest snapshot.
   *
   * Snapshot keys are formatted as `snapshot:<epochMillis>`. They are compared
   * by the numeric timestamp extracted from each key rather than sorted as
   * strings, because lexicographic ordering breaks once timestamps differ in
   * length (e.g. `snapshot:9999999999999` would sort after
   * `snapshot:10000000000000`).
   */
  static async getLatestSnapshot(): Promise<SystemMetrics | null> {
    const client = getRedisClient();
    const keys = await client.keys(`${SNAPSHOT_PREFIX}*`);

    if (keys.length === 0) return null;

    // Pick the key with the largest numeric timestamp.
    const latestKey = keys.reduce((latest, key) =>
      this._extractSnapshotTimestamp(key) > this._extractSnapshotTimestamp(latest) ? key : latest
    );

    const data = await client.get(latestKey);
    return data ? JSON.parse(data) : null;
  }

  /**
   * Extract the epoch-millisecond timestamp from a snapshot key.
   */
  private static _extractSnapshotTimestamp(key: string): number {
    return parseInt(key.slice(SNAPSHOT_PREFIX.length), 10) || 0;
  }

  /**
   * Get health status
   */
  static async getHealthStatus() {
    try {
      const snapshot = await this.getLatestSnapshot();

      if (!snapshot) {
        return {
          status: 'unknown',
          message: 'No metrics available',
        };
      }

      const workerCount = Object.keys(snapshot.workers).length;
      const onlineWorkers = Object.values(snapshot.workers).filter((w: any) => w.status === 'online').length;
      const queueCount = Object.keys(snapshot.queues).length;
      const dlqSize = snapshot.tasks.deadLetterQueueSize || 0;

      let health = 'healthy';
      const issues: string[] = [];

      if (onlineWorkers === 0) {
        health = 'critical';
        issues.push('No online workers');
      } else if (onlineWorkers < Math.ceil(workerCount * 0.5)) {
        health = 'degraded';
        issues.push('Less than 50% workers online');
      }

      if (dlqSize > 100) {
        health = health === 'critical' ? 'critical' : 'degraded';
        issues.push(`High DLQ size: ${dlqSize}`);
      }

      return {
        status: health,
        workers: {
          online: onlineWorkers,
          total: workerCount,
        },
        queues: queueCount,
        dlqSize,
        issues,
        timestamp: snapshot.timestamp,
      };
    } catch (error) {
      logger.error({ error }, 'Health check error');
      return {
        status: 'error',
        message: String(error),
      };
    }
  }

  /**
   * Get detailed queue metrics
   */
  static async getDetailedQueueMetrics() {
    const snapshot = await this.getLatestSnapshot();

    if (!snapshot) {
      return {};
    }

    return snapshot.queues;
  }

  /**
   * Get worker performance metrics
   */
  static async getWorkerPerformance() {
    const snapshot = await this.getLatestSnapshot();

    if (!snapshot) {
      return {};
    }

    return Object.entries(snapshot.workers).map(([_, metrics]: any) => ({
      ...metrics,
      efficiency: metrics.successRate / 100,
    }));
  }
}
