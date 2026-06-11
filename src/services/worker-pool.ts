import { v4 as uuidv4 } from 'uuid';
import { getRedisClient } from './redis';
import logger from '../utils/logger';
import { TaskQueue } from './task-queue';
import { Worker, WorkerStatus, Task, TaskExecutionMetrics } from '../types';

const WORKER_PREFIX = 'worker:';
const WORKERS_INDEX = 'workers:index';
const WORKER_HANDLERS = 'worker:handlers:map';
const METRICS_PREFIX = 'metrics:worker:';

export interface AutoScaleDecision {
  action: 'scale_up' | 'scale_down' | 'none';
  reason: string;
}

export class WorkerPool {
  /**
   * Auto-scaling thresholds (configurable).
   * - scaleUpQueueDepth: queue size at which to add capacity
   * - saturatedCapacityPct: average worker capacity considered "full"
   * - cooldownMs: minimum time between scale events to prevent thrashing
   */
  static autoScaleConfig = {
    scaleUpQueueDepth: 50,
    saturatedCapacityPct: 80,
    cooldownMs: 30_000,
  };

  private static lastScaleEventAt = 0;

  /**
   * Register a new worker
   */
  static async registerWorker(
    name: string,
    handlers: string[],
    options: {
      maxConcurrent?: number;
      version?: string;
      tags?: string[];
    } = {}
  ): Promise<Worker> {
    const client = getRedisClient();
    const workerId = uuidv4();

    const worker: Worker = {
      id: workerId,
      name,
      status: 'online',
      handlers,
      maxConcurrent: options.maxConcurrent || 5,
      currentTasks: 0,
      totalProcessed: 0,
      totalFailed: 0,
      lastHeartbeat: new Date(),
      registeredAt: new Date(),
      version: options.version || '1.0.0',
      capacity: 0,
      tags: options.tags || [],
    };

    await client.set(`${WORKER_PREFIX}${workerId}`, JSON.stringify(worker));
    await client.zAdd(WORKERS_INDEX, { score: Date.now(), value: workerId });

    // Map handlers to worker
    for (const handler of handlers) {
      await client.sAdd(`${WORKER_HANDLERS}:${handler}`, workerId);
    }

    logger.info({ workerId, name, handlers }, 'Worker registered');
    return worker;
  }

  /**
   * Get worker by ID
   */
  static async getWorker(workerId: string): Promise<Worker | null> {
    const client = getRedisClient();
    const data = await client.get(`${WORKER_PREFIX}${workerId}`);
    return data ? JSON.parse(data) : null;
  }

  /**
   * Update worker status
   */
  static async updateWorkerStatus(workerId: string, status: WorkerStatus): Promise<void> {
    const client = getRedisClient();
    const worker = await this.getWorker(workerId);

    if (!worker) {
      throw new Error(`Worker ${workerId} not found`);
    }

    worker.status = status;
    worker.lastHeartbeat = new Date();
    worker.capacity = Math.round((worker.currentTasks / worker.maxConcurrent) * 100);

    await client.set(`${WORKER_PREFIX}${workerId}`, JSON.stringify(worker));
  }

  /**
   * Get available workers for a specific handler
   */
  static async getAvailableWorkers(handler: string): Promise<Worker[]> {
    const client = getRedisClient();
    const workerIds = await client.sMembers(`${WORKER_HANDLERS}:${handler}`);

    const availableWorkers: Worker[] = [];

    for (const workerId of workerIds) {
      const worker = await this.getWorker(workerId);
      if (
        worker &&
        worker.status === 'online' &&
        worker.currentTasks < worker.maxConcurrent
      ) {
        availableWorkers.push(worker);
      }
    }

    // Sort by capacity (least busy first)
    availableWorkers.sort((a, b) => a.capacity - b.capacity);
    return availableWorkers;
  }

  /**
   * Assign task to worker
   */
  static async assignTask(workerId: string, task: Task): Promise<void> {
    const client = getRedisClient();
    const worker = await this.getWorker(workerId);

    if (!worker) {
      throw new Error(`Worker ${workerId} not found`);
    }

    worker.currentTasks++;
    worker.capacity = Math.round((worker.currentTasks / worker.maxConcurrent) * 100);

    await client.set(`${WORKER_PREFIX}${workerId}`, JSON.stringify(worker));
    await client.lPush(`worker:${workerId}:tasks`, task.id);

    logger.info({ workerId, taskId: task.id }, 'Task assigned to worker');
  }

  /**
   * Complete task on worker
   */
  static async completeTask(
    workerId: string,
    taskId: string,
    metrics: Partial<TaskExecutionMetrics>
  ): Promise<void> {
    const client = getRedisClient();
    const worker = await this.getWorker(workerId);

    if (!worker) {
      throw new Error(`Worker ${workerId} not found`);
    }

    worker.currentTasks = Math.max(0, worker.currentTasks - 1);
    worker.totalProcessed++;

    if (!metrics.success) {
      worker.totalFailed++;
    }

    worker.capacity = Math.round((worker.currentTasks / worker.maxConcurrent) * 100);

    await client.set(`${WORKER_PREFIX}${workerId}`, JSON.stringify(worker));
    await client.lRem(`worker:${workerId}:tasks`, 1, taskId);

    // Store metrics
    const metricsKey = `${METRICS_PREFIX}${workerId}:${Date.now()}`;
    await client.set(
      metricsKey,
      JSON.stringify({
        workerId,
        taskId,
        ...metrics,
      }),
      { EX: 7 * 24 * 60 * 60 } // 7 days retention
    );
  }

  /**
   * Heartbeat to keep worker alive
   */
  static async heartbeat(workerId: string): Promise<boolean> {
    const client = getRedisClient();
    const worker = await this.getWorker(workerId);

    if (!worker) {
      return false;
    }

    worker.lastHeartbeat = new Date();
    await client.set(`${WORKER_PREFIX}${workerId}`, JSON.stringify(worker));
    return true;
  }

  /**
   * Check and mark stale workers as offline
   */
  static async checkStaleWorkers(timeoutSeconds: number = 60): Promise<number> {
    const client = getRedisClient();
    const allWorkers = await client.zRange(WORKERS_INDEX, 0, -1);

    let staleCount = 0;
    const now = Date.now();
    const timeout = timeoutSeconds * 1000;

    for (const workerId of allWorkers) {
      const worker = await this.getWorker(workerId);
      if (worker && now - new Date(worker.lastHeartbeat).getTime() > timeout) {
        // Mark offline and reassign any tasks the worker was processing.
        await this.handleWorkerDisconnect(workerId);
        staleCount++;
      }
    }

    return staleCount;
  }

  /**
   * Handle a worker disconnecting (crash or missed heartbeats): requeue any
   * tasks it was processing so other workers can pick them up, clear its task
   * assignment and counters, and mark it offline. Returns the number of tasks
   * reassigned.
   */
  static async handleWorkerDisconnect(workerId: string): Promise<number> {
    const client = getRedisClient();
    const taskListKey = `worker:${workerId}:tasks`;

    const taskIds = await client.lRange(taskListKey, 0, -1);
    let reassigned = 0;
    for (const taskId of taskIds) {
      const requeued = await TaskQueue.requeueTask(taskId);
      if (requeued) {
        reassigned++;
      }
    }
    await client.del(taskListKey);

    const worker = await this.getWorker(workerId);
    if (worker) {
      worker.status = 'offline';
      worker.currentTasks = 0;
      worker.capacity = 0;
      await client.set(`${WORKER_PREFIX}${workerId}`, JSON.stringify(worker));
    }

    logger.warn({ workerId, reassigned }, 'Worker disconnected; tasks reassigned and worker marked offline');
    return reassigned;
  }

  /**
   * Get worker metrics
   */
  static async getWorkerMetrics(workerId: string) {
    const client = getRedisClient();
    const worker = await this.getWorker(workerId);

    if (!worker) {
      throw new Error(`Worker ${workerId} not found`);
    }

    const successRate =
      worker.totalProcessed > 0
        ? ((worker.totalProcessed - worker.totalFailed) / worker.totalProcessed) * 100
        : 0;

    return {
      workerId,
      name: worker.name,
      status: worker.status,
      currentTasks: worker.currentTasks,
      totalProcessed: worker.totalProcessed,
      totalFailed: worker.totalFailed,
      successRate: successRate.toFixed(2),
      capacity: worker.capacity,
      handlers: worker.handlers,
      lastHeartbeat: worker.lastHeartbeat,
    };
  }

  /**
   * Average capacity (0-100) across non-offline workers.
   */
  static async getAverageCapacity(): Promise<number> {
    const client = getRedisClient();
    const workerIds = await client.zRange(WORKERS_INDEX, 0, -1);

    let total = 0;
    let count = 0;
    for (const workerId of workerIds) {
      const worker = await this.getWorker(workerId);
      if (worker && worker.status !== 'offline') {
        total += worker.capacity;
        count++;
      }
    }

    return count === 0 ? 0 : Math.round(total / count);
  }

  /**
   * Decide whether to scale workers up or down based on queue depth and
   * average capacity. Scales up when the queue is backing up and workers are
   * saturated, scales down when the queue is empty, and otherwise does nothing.
   * A cooldown between scale events prevents thrashing.
   *
   * Returns the decision; actual provisioning of workers is left to the
   * deployment/orchestration layer that consumes this signal.
   */
  static evaluateAutoScaling(
    queueDepth: number,
    avgCapacity: number,
    now: number = Date.now()
  ): AutoScaleDecision {
    const cfg = this.autoScaleConfig;

    if (now - this.lastScaleEventAt < cfg.cooldownMs) {
      return { action: 'none', reason: 'cooldown active' };
    }

    if (queueDepth >= cfg.scaleUpQueueDepth && avgCapacity >= cfg.saturatedCapacityPct) {
      this.lastScaleEventAt = now;
      return {
        action: 'scale_up',
        reason: `queue depth ${queueDepth} >= ${cfg.scaleUpQueueDepth} and capacity ${avgCapacity}% saturated`,
      };
    }

    if (queueDepth === 0) {
      this.lastScaleEventAt = now;
      return { action: 'scale_down', reason: 'queue empty' };
    }

    return { action: 'none', reason: 'within thresholds' };
  }

  /**
   * Unregister worker
   */
  static async unregisterWorker(workerId: string): Promise<void> {
    const client = getRedisClient();
    const worker = await this.getWorker(workerId);

    if (!worker) {
      throw new Error(`Worker ${workerId} not found`);
    }

    // Remove from handlers map
    for (const handler of worker.handlers) {
      await client.sRem(`${WORKER_HANDLERS}:${handler}`, workerId);
    }

    await client.del(`${WORKER_PREFIX}${workerId}`);
    await client.zRem(WORKERS_INDEX, workerId);

    logger.info({ workerId }, 'Worker unregistered');
  }
}
