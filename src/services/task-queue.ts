import { v4 as uuidv4 } from 'uuid';
import { getRedisClient } from './redis';
import logger from '../utils/logger';
import { Task, TaskStatus, Queue, RecurrenceRule } from '../types';

const TASK_PREFIX = 'task:';
const QUEUE_PREFIX = 'queue:';
const QUEUE_LIST_KEY = 'queues:all';
const TASK_INDEX_KEY = 'tasks:index';
const DEAD_LETTER_QUEUE = 'dlq:tasks';

/**
 * Thrown when a queue has reached its maximum capacity (backpressure).
 * The API layer maps this to HTTP 429 Too Many Requests.
 */
export class QueueFullError extends Error {
  constructor(queueName: string, maxSize: number) {
    super(`Queue ${queueName} exceeds maximum size of ${maxSize}`);
    this.name = 'QueueFullError';
  }
}

export class TaskQueue {
  /**
   * Create a new task and add it to the queue
   */
  static async createTask(
    name: string,
    handler: string,
    payload: Record<string, any>,
    options: {
      queueName?: string;
      priority?: 'low' | 'medium' | 'high' | 'critical';
      maxRetries?: number;
      timeout?: number;
      dependencies?: string[];
      scheduledFor?: Date;
      recurrence?: RecurrenceRule;
      tags?: string[];
      metadata?: Record<string, any>;
    } = {}
  ): Promise<Task> {
    const client = getRedisClient();
    const taskId = uuidv4();
    const queueName = options.queueName || 'default';

    const queueKey = `${QUEUE_PREFIX}${queueName}`;
    const maxQueueSize = parseInt(process.env.MAX_QUEUE_SIZE || '10000', 10);
    const currentSize = await client.zCard(queueKey);

    if (currentSize >= maxQueueSize) {
      throw new QueueFullError(queueName, maxQueueSize);
    }

    const task: Task = {
      id: taskId,
      name,
      description: `Task: ${name}`,
      priority: options.priority || 'medium',
      status: 'pending',
      handler,
      payload,
      retries: 0,
      maxRetries: options.maxRetries || 3,
      timeout: options.timeout || 30000,
      createdAt: new Date(),
      queue: queueName,
      dependencies: options.dependencies || [],
      scheduledFor: options.scheduledFor,
      recurrence: options.recurrence,
      tags: options.tags || [],
      metadata: options.metadata || {},
    };

    // Store task
    await client.set(`${TASK_PREFIX}${taskId}`, JSON.stringify(task));

    // Add to task index
    await client.zAdd(TASK_INDEX_KEY, { score: Date.now(), value: taskId });

    // Add to queue
    const score = this._calculateQueueScore(task.priority);
    await client.zAdd(queueKey, { score, value: taskId });

    // Update queue metadata
    await this._updateQueueStats(queueName, 1);

    logger.info({ taskId, queueName }, 'Task created');
    return task;
  }

  /**
   * Create multiple tasks in one call.
   *
   * Validates every input up front (non-empty, within the 1000-task limit, each
   * with a name and handler) so the batch is rejected before anything is
   * written if any entry is invalid. Returns the created tasks in order.
   */
  static async createTasksBatch(
    inputs: Array<{
      name: string;
      handler: string;
      payload?: Record<string, any>;
      options?: Parameters<typeof TaskQueue.createTask>[3];
    }>
  ): Promise<Task[]> {
    if (inputs.length === 0) {
      throw new Error('Batch must contain at least one task');
    }
    if (inputs.length > 1000) {
      throw new Error('Batch size exceeds the maximum of 1000 tasks');
    }

    inputs.forEach((input, index) => {
      if (!input.name || !input.handler) {
        throw new Error(`Invalid task at index ${index}: name and handler are required`);
      }
    });

    const created: Task[] = [];
    for (const input of inputs) {
      created.push(await this.createTask(input.name, input.handler, input.payload || {}, input.options || {}));
    }

    logger.info({ count: created.length }, 'Batch of tasks created');
    return created;
  }

  /**
   * Get task by ID
   */
  static async getTask(taskId: string): Promise<Task | null> {
    const client = getRedisClient();
    const data = await client.get(`${TASK_PREFIX}${taskId}`);
    return data ? JSON.parse(data) : null;
  }

  /**
   * Update task status
   */
  static async updateTaskStatus(taskId: string, status: TaskStatus, metadata?: Record<string, any>): Promise<void> {
    const client = getRedisClient();
    const task = await this.getTask(taskId);

    if (!task) {
      throw new Error(`Task ${taskId} not found`);
    }

    const previousStatus = task.status;

    task.status = status;
    if (metadata) {
      Object.assign(task, metadata);
    }

    if (status === 'completed') {
      task.completedAt = new Date();
    } else if (status === 'processing') {
      task.startedAt = new Date();
    }

    await client.set(`${TASK_PREFIX}${taskId}`, JSON.stringify(task));

    // Keep queue stats consistent as the task moves between states.
    if (previousStatus !== status) {
      await this._transitionQueueStats(task.queue, previousStatus, status);
    }

    logger.info({ taskId, status }, 'Task status updated');
  }

  /**
   * Get next task from queue.
   *
   * Priority semantics: tasks are ordered by a priority-derived score
   * (critical > high > medium > low) in the queue sorted set, and this returns
   * the highest-priority task that is actually runnable (dependencies met and
   * not scheduled for the future). The full queue is scanned in priority order
   * rather than only the top N, so a runnable high-priority task is never
   * skipped because lower-priority or blocked tasks happen to sort ahead in a
   * truncated window — i.e. no priority inversion.
   */
  static async getNextTask(queueName: string): Promise<Task | null> {
    const client = getRedisClient();
    const queueKey = `${QUEUE_PREFIX}${queueName}`;

    // Scan the whole queue from highest to lowest priority.
    const taskIds = await client.zRange(queueKey, 0, -1, { REV: true });

    for (const taskId of taskIds) {
      const task = await this.getTask(taskId);
      if (!task) continue;

      // Skip if dependencies not met
      if (task.dependencies.length > 0) {
        const depsResolved = await this._checkDependencies(task.dependencies);
        if (!depsResolved) continue;
      }

      // Skip if scheduled for later
      if (task.scheduledFor && new Date(task.scheduledFor) > new Date()) {
        continue;
      }

      return task;
    }

    return null;
  }

  /**
   * Retry a failed task
   */
  static async retryTask(taskId: string): Promise<boolean> {
    const client = getRedisClient();
    const task = await this.getTask(taskId);

    if (!task) {
      throw new Error(`Task ${taskId} not found`);
    }

    if (task.retries >= task.maxRetries) {
      await this._moveToDeadLetterQueue(taskId);
      return false;
    }

    task.retries += 1;
    task.status = 'retry';
    delete task.error;

    await client.set(`${TASK_PREFIX}${taskId}`, JSON.stringify(task));

    const queueKey = `${QUEUE_PREFIX}${task.queue}`;
    const score = this._calculateQueueScore(task.priority);
    await client.zAdd(queueKey, { score, value: taskId });

    logger.info({ taskId, attempt: task.retries }, 'Task retry queued');
    return true;
  }

  /**
   * Get all tasks from queue
   */
  static async getQueueTasks(queueName: string, limit: number = 100, offset: number = 0): Promise<Task[]> {
    const client = getRedisClient();
    const queueKey = `${QUEUE_PREFIX}${queueName}`;

    const taskIds = await client.zRange(queueKey, offset, offset + limit - 1, { REV: true });
    const tasks: Task[] = [];

    for (const taskId of taskIds) {
      const task = await this.getTask(taskId);
      if (task) tasks.push(task);
    }

    return tasks;
  }

  /**
   * Get queue statistics
   */
  static async getQueueStats(queueName: string) {
    const client = getRedisClient();
    const queueKey = `${QUEUE_PREFIX}${queueName}`;
    const statsKey = `${QUEUE_PREFIX}${queueName}:stats`;

    const queueSize = await client.zCard(queueKey);
    const stats = await client.hGetAll(statsKey);

    return {
      queueName,
      queueSize,
      pending: parseInt(stats.pending || '0'),
      processing: parseInt(stats.processing || '0'),
      completed: parseInt(stats.completed || '0'),
      failed: parseInt(stats.failed || '0'),
    };
  }

  /**
   * Clean up old completed tasks
   */
  static async cleanupOldTasks(hoursAgo: number = 24): Promise<number> {
    const client = getRedisClient();
    const cutoffTime = Date.now() - hoursAgo * 60 * 60 * 1000;

    const allTaskIds = await client.zRange(TASK_INDEX_KEY, cutoffTime, 0, { BY: 'SCORE', REV: true });
    let deleted = 0;

    for (const taskId of allTaskIds) {
      const task = await this.getTask(taskId);
      if (task && (task.status === 'completed' || task.status === 'failed')) {
        await client.del(`${TASK_PREFIX}${taskId}`);
        await client.zRem(TASK_INDEX_KEY, taskId);
        deleted++;
      }
    }

    logger.info({ deleted, hoursAgo }, 'Old tasks cleaned up');
    return deleted;
  }

  /**
   * Recover tasks orphaned by a crashed worker.
   *
   * A task whose worker dies stays in "processing" forever. This finds tasks
   * that have been processing longer than `staleMs`, resets them to "queued",
   * detaches the dead worker, and re-enqueues them so another worker can pick
   * them up. Returns the number of tasks recovered.
   */
  static async recoverStaleTasks(staleMs: number = 5 * 60 * 1000): Promise<number> {
    const client = getRedisClient();
    const taskIds = await client.zRange(TASK_INDEX_KEY, 0, -1);
    const now = Date.now();
    let recovered = 0;

    for (const taskId of taskIds) {
      const task = await this.getTask(taskId);
      if (!task || task.status !== 'processing') {
        continue;
      }

      const startedAt = task.startedAt ? new Date(task.startedAt).getTime() : 0;
      if (startedAt === 0 || now - startedAt < staleMs) {
        continue;
      }

      const previousWorker = task.workerId;
      task.status = 'queued';
      task.workerId = undefined;
      await client.set(`${TASK_PREFIX}${taskId}`, JSON.stringify(task));

      const queueKey = `${QUEUE_PREFIX}${task.queue}`;
      const score = this._calculateQueueScore(task.priority);
      await client.zAdd(queueKey, { score, value: taskId });

      recovered++;
      logger.warn(
        { taskId, previousWorker, stalledForMs: now - startedAt },
        'Recovered orphaned task stuck in processing'
      );
    }

    if (recovered > 0) {
      logger.info({ recovered }, 'Orphaned tasks recovered');
    }
    return recovered;
  }

  // Private helper methods

  private static _calculateQueueScore(priority: string): number {
    const priorityMap: Record<string, number> = {
      critical: 1000,
      high: 100,
      medium: 10,
      low: 1,
    };
    return (priorityMap[priority] || 10) + Math.random();
  }

  private static async _checkDependencies(dependencyIds: string[]): Promise<boolean> {
    for (const depId of dependencyIds) {
      const task = await this.getTask(depId);
      if (!task || task.status !== 'completed') {
        return false;
      }
    }
    return true;
  }

  private static async _moveToDeadLetterQueue(taskId: string): Promise<void> {
    const client = getRedisClient();
    const task = await this.getTask(taskId);

    if (task) {
      task.status = 'failed';
      await client.lPush(DEAD_LETTER_QUEUE, JSON.stringify(task));
      await client.del(`${TASK_PREFIX}${taskId}`);
      logger.warn({ taskId }, 'Task moved to DLQ');
    }
  }

  private static async _updateQueueStats(queueName: string, increment: number): Promise<void> {
    const client = getRedisClient();
    const statsKey = `${QUEUE_PREFIX}${queueName}:stats`;
    await client.hIncrBy(statsKey, 'pending', increment);
  }

  // Task statuses that are tracked as queue stat counters.
  private static readonly STATS_BUCKETS: ReadonlyArray<TaskStatus> = [
    'pending',
    'processing',
    'completed',
    'failed',
  ];

  /**
   * Move a task between queue stat counters when its status changes,
   * decrementing the previous bucket and incrementing the new one so the
   * counters stay consistent (e.g. pending -> processing -> completed).
   */
  private static async _transitionQueueStats(
    queueName: string,
    from: TaskStatus,
    to: TaskStatus
  ): Promise<void> {
    const client = getRedisClient();
    const statsKey = `${QUEUE_PREFIX}${queueName}:stats`;

    if (this.STATS_BUCKETS.includes(from)) {
      await client.hIncrBy(statsKey, from, -1);
    }
    if (this.STATS_BUCKETS.includes(to)) {
      await client.hIncrBy(statsKey, to, 1);
    }
  }
}
