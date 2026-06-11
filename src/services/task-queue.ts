import { v4 as uuidv4 } from 'uuid';
import { getRedisClient } from './redis';
import logger from '../utils/logger';
import { Task, TaskStatus, Queue, RecurrenceRule } from '../types';

const TASK_PREFIX = 'task:';
const QUEUE_PREFIX = 'queue:';
const QUEUE_LIST_KEY = 'queues:all';
const TASK_INDEX_KEY = 'tasks:index';
const DEAD_LETTER_QUEUE = 'dlq:tasks';

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
    const queueKey = `${QUEUE_PREFIX}${queueName}`;
    const score = this._calculateQueueScore(task.priority);
    await client.zAdd(queueKey, { score, value: taskId });

    // Update queue metadata
    await this._updateQueueStats(queueName, 1);

    logger.info({ taskId, queueName }, 'Task created');
    return task;
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
   * Get next task from queue (considering priority and dependencies)
   */
  static async getNextTask(queueName: string): Promise<Task | null> {
    const client = getRedisClient();
    const queueKey = `${QUEUE_PREFIX}${queueName}`;

    const taskIds = await client.zRange(queueKey, 0, 9, { REV: true }); // Get top 10 by priority

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
