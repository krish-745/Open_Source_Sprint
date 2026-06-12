import { v4 as uuidv4 } from 'uuid';
import { WatchError } from 'redis';
import { getRedisClient } from './redis';
import logger from '../utils/logger';
import { Task, TaskStatus, Queue, RecurrenceRule, TaskBranch } from '../types';

const TASK_PREFIX = 'task:';
const QUEUE_PREFIX = 'queue:';
const QUEUE_LIST_KEY = 'queues:all';
const TASK_INDEX_KEY = 'tasks:index';
const DEAD_LETTER_QUEUE = 'dlq:tasks';
const TASK_STATUS_INDEX_PREFIX = 'tasks:status:';
const TASK_QUEUE_STATUS_INDEX_PREFIX = 'tasks:queue:';

/**
 * Thrown when a task's dependencies would introduce a circular dependency.
 * Carries the offending cycle (as a list of task ids) for the caller to report.
 */
export class DependencyCycleError extends Error {
  constructor(message: string, public readonly cycle: string[]) {
    super(message);
    this.name = 'DependencyCycleError';
  }
}

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
   * Create a new task and add it to the queue.
   * The payload must be a valid JSON object. If null or undefined is provided, it defaults to an empty object.
   */
  static async createTask(
    name: string,
    handler: string,
    payload: Record<string, any> | null | undefined,
    options: {
      queueName?: string;
      priority?: 'low' | 'medium' | 'high' | 'critical';
      maxRetries?: number;
      timeout?: number;
      dependencies?: string[];
      scheduledFor?: Date;
      callbackUrl?: string;
      recurrence?: RecurrenceRule;
      tags?: string[];
      metadata?: Record<string, any>;
    } = {}
  ): Promise<Task> {
    const client = getRedisClient();
    const taskId = uuidv4();
    const queueName = options.queueName || 'default';

    if (payload !== undefined && payload !== null && (typeof payload !== 'object' || Array.isArray(payload))) {
      throw new Error('Payload must be a valid object');
    }

    const finalPayload = payload || {};

    const queueKey = `${QUEUE_PREFIX}${queueName}`;
    const maxQueueSize = parseInt(process.env.MAX_QUEUE_SIZE || '10000', 10);
    const currentSize = await client.zCard(queueKey);

    if (currentSize >= maxQueueSize) {
      throw new QueueFullError(queueName, maxQueueSize);
    }
    
    const createdAt = new Date();
    const task: Task = {
      id: taskId,
      name,
      description: `Task: ${name}`,
      priority: options.priority || 'medium',
      status: 'pending',
      handler,
      payload: finalPayload,
      retries: 0,
      maxRetries: options.maxRetries || 3,
      timeout: options.timeout || 30000,
      createdAt,
      queue: queueName,
      dependencies: options.dependencies || [],
      scheduledFor: options.scheduledFor,
      callbackUrl: options.callbackUrl,
      recurrence: options.recurrence,
      tags: options.tags || [],
      metadata: options.metadata || {},
    };

    // Reject tasks that would introduce a circular dependency.
    if (task.dependencies.length > 0) {
      const cycle = await this._detectDependencyCycle(taskId, task.dependencies);
      if (cycle) {
        throw new DependencyCycleError(
          `Circular dependency detected: ${cycle.join(' -> ')}`,
          cycle
        );
      }
    }

    const timestamp = createdAt.getTime();

    // Store task
    await client.set(`${TASK_PREFIX}${taskId}`, JSON.stringify(task));

    // Add to task index
    await client.zAdd(TASK_INDEX_KEY, { score: timestamp, value: taskId });

    // Add to secondary indices (status and queue+status)
    await this._updateTaskIndices(taskId, queueName, timestamp, task.status);

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
   * Evaluate a task's conditional branches against its result
   */
  static evaluateBranches(task: Task, result: any): TaskBranch[] {
    if (!task.branches || task.branches.length === 0) {
      return [];
    }
    const resultStr = typeof result === 'string' ? result : JSON.stringify(result ?? '');

    return task.branches.filter((branch) => {
      if (branch.condition.startsWith('regex:')) {
        try {
          return new RegExp(branch.condition.slice('regex:'.length)).test(resultStr);
        } catch {
          return false; // invalid regex never matches
        }
      }
      return resultStr.includes(branch.condition);
    });
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
   * Modify a not-yet-executed task's parameters.
   *
   * Only the whitelisted fields (`priority`, `timeout`, `maxRetries`, `tags`,
   * `scheduledFor`) may be changed, and only while the task has not started
   * (status pending/queued/blocked). An audit entry is appended to the task's
   * metadata for every change. Returns the updated task; throws if the task is
   * missing or already executing/finished.
   */
  static async updateTaskFields(
    taskId: string,
    fields: {
      priority?: 'low' | 'medium' | 'high' | 'critical';
      timeout?: number;
      maxRetries?: number;
      tags?: string[];
      scheduledFor?: Date;
    }
  ): Promise<Task> {
    const client = getRedisClient();
    const task = await this.getTask(taskId);

    if (!task) {
      throw new Error(`Task ${taskId} not found`);
    }

    const modifiable = ['pending', 'queued', 'blocked'];
    if (!modifiable.includes(task.status)) {
      throw new Error(`Task ${taskId} cannot be modified in status '${task.status}'`);
    }

    if (fields.timeout !== undefined && fields.timeout <= 0) {
      throw new Error('timeout must be positive');
    }
    if (fields.maxRetries !== undefined && fields.maxRetries < 0) {
      throw new Error('maxRetries must be non-negative');
    }

    const changes: Record<string, { from: any; to: any }> = {};
    const apply = <K extends keyof Task>(key: K, value: Task[K] | undefined) => {
      if (value !== undefined && task[key] !== value) {
        changes[key as string] = { from: task[key], to: value };
        task[key] = value;
      }
    };

    apply('priority', fields.priority);
    apply('timeout', fields.timeout);
    apply('maxRetries', fields.maxRetries);
    apply('tags', fields.tags);
    apply('scheduledFor', fields.scheduledFor);

    if (Object.keys(changes).length > 0) {
      const audit = (task.metadata.auditLog as any[]) || [];
      audit.push({ at: new Date().toISOString(), changes });
      task.metadata.auditLog = audit;

      await client.set(`${TASK_PREFIX}${taskId}`, JSON.stringify(task));

      // Re-score the queue entry if priority changed.
      if (changes.priority) {
        const queueKey = `${QUEUE_PREFIX}${task.queue}`;
        await client.zAdd(queueKey, { score: this._calculateQueueScore(task.priority), value: taskId });
      }

      logger.info({ taskId, changed: Object.keys(changes) }, 'Task fields updated');
    }

    return task;
  }

  /**
   * Update task status atomically.
   */
  static async updateTaskStatus(taskId: string, status: TaskStatus, metadata?: Record<string, any>): Promise<void> {
    const client = getRedisClient();
    const key = `${TASK_PREFIX}${taskId}`;
    const maxAttempts = 5;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      await client.watch(key);

      const data = await client.get(key);
      if (!data) {
        await client.unwatch();
        throw new Error(`Task ${taskId} not found`);
      }

      const task: Task = JSON.parse(data);
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

      try {
        const timestamp = new Date(task.createdAt).getTime();
        const multi = client.multi().set(key, JSON.stringify(task));

        // Incorporate index updates directly into the optimistic locking transaction
        if (previousStatus !== status) {
          multi.zRem(`${TASK_STATUS_INDEX_PREFIX}${previousStatus}`, taskId);
          multi.zRem(`${TASK_QUEUE_STATUS_INDEX_PREFIX}${task.queue}:status:${previousStatus}`, taskId);
          multi.zAdd(`${TASK_STATUS_INDEX_PREFIX}${status}`, { score: timestamp, value: taskId });
          multi.zAdd(`${TASK_QUEUE_STATUS_INDEX_PREFIX}${task.queue}:status:${status}`, { score: timestamp, value: taskId });
        }

        await multi.exec();

        // Keep queue stats consistent as the task moves between states.
        if (previousStatus !== status) {
          await this._transitionQueueStats(task.queue, previousStatus, status);
        }

        logger.info({ taskId, status }, 'Task status updated');
        return;
      } catch (error) {
        if (error instanceof WatchError) {
          logger.warn({ taskId, attempt }, 'Concurrent task update detected, retrying');
          continue;
        }
        throw error;
      }
    }

    throw new Error(`Failed to update task ${taskId} after ${maxAttempts} attempts due to concurrent modifications`);
  }

  /**
   * Get next task from queue.
   */
  static async getNextTask(queueName: string): Promise<Task | null> {
    const client = getRedisClient();
    const queueKey = `${QUEUE_PREFIX}${queueName}`;

    const CHUNK_SIZE = 50;
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const taskIds = await client.zRange(queueKey, offset, offset + CHUNK_SIZE - 1, { REV: true });
      if (taskIds.length === 0) break;

      for (const taskId of taskIds) {
        const task = await this.getTask(taskId);
        if (!task) continue;

        if (task.dependencies.length > 0) {
          const depsResolved = await this._checkDependencies(task.dependencies);
          if (!depsResolved) continue;
        }

        if (task.scheduledFor && new Date(task.scheduledFor) > new Date()) {
          continue;
        }

        return task;
      }

      offset += CHUNK_SIZE;
      hasMore = taskIds.length === CHUNK_SIZE;
    }

    return null;
  }

  /**
   * Fetch up to `batchSize` runnable tasks from a queue in priority order.
   */
  static async getNextBatch(queueName: string, batchSize: number = 10): Promise<Task[]> {
    if (batchSize < 1) return [];
    
    const client = getRedisClient();
    const queueKey = `${QUEUE_PREFIX}${queueName}`;
    const batch: Task[] = [];
    
    const CHUNK_SIZE = 50;
    let offset = 0;
    let hasMore = true;

    while (hasMore && batch.length < batchSize) {
      const taskIds = await client.zRange(queueKey, offset, offset + CHUNK_SIZE - 1, { REV: true });
      if (taskIds.length === 0) break;

      for (const taskId of taskIds) {
        if (batch.length >= batchSize) break;

        const task = await this.getTask(taskId);
        if (!task) continue;

        if (task.dependencies.length > 0) {
          const depsResolved = await this._checkDependencies(task.dependencies);
          if (!depsResolved) continue;
        }
        if (task.scheduledFor && new Date(task.scheduledFor) > new Date()) {
          continue;
        }

        batch.push(task);
      }
      
      offset += CHUNK_SIZE;
      hasMore = taskIds.length === CHUNK_SIZE;
    }

    return batch;
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

    const previousStatus = task.status;
    task.retries += 1;
    task.status = 'retry';
    delete task.error;

    await client.set(`${TASK_PREFIX}${taskId}`, JSON.stringify(task));

    // Synchronize indices safely
    await this._updateTaskIndices(
      taskId, 
      task.queue, 
      new Date(task.createdAt).getTime(), 
      task.status, 
      previousStatus
    );

    const queueKey = `${QUEUE_PREFIX}${task.queue}`;
    const score = this._calculateQueueScore(task.priority);
    await client.zAdd(queueKey, { score, value: taskId });

    logger.info({ taskId, attempt: task.retries }, 'Task retry queued');
    return true;
  }

  /**
   * Cancel a task.
   */
  static async cancelTask(taskId: string): Promise<boolean> {
    const client = getRedisClient();
    const task = await this.getTask(taskId);

    if (!task) {
      throw new Error(`Task ${taskId} not found`);
    }

    if (['completed', 'failed', 'cancelled'].includes(task.status)) {
      return false;
    }

    const previousStatus = task.status;
    task.status = 'cancelled';
    await client.set(`${TASK_PREFIX}${taskId}`, JSON.stringify(task));
    await client.zRem(`${QUEUE_PREFIX}${task.queue}`, taskId);

    // Synchronize indices safely
    await this._updateTaskIndices(
      taskId, 
      task.queue, 
      new Date(task.createdAt).getTime(), 
      task.status, 
      previousStatus
    );

    logger.info({ taskId }, 'Task cancelled');
    return true;
  }

  /**
   * Get all tasks from queue
   */
  static async getQueueTasks(queueName: string, limit: number = 100, offset: number = 0): Promise<Task[]> {
    const client = getRedisClient();
    const queueKey = `${QUEUE_PREFIX}${queueName}`;

    const taskIds = await client.zRange(queueKey, offset, offset + limit - 1, { REV: true });
    return this._fetchTasksByIds(taskIds);
  }

  /**
   * Query tasks by status globally (O(log n))
   */
  static async getTasksByStatus(status: TaskStatus, limit: number = 100, offset: number = 0): Promise<Task[]> {
    const client = getRedisClient();
    const key = `${TASK_STATUS_INDEX_PREFIX}${status}`;
    const taskIds = await client.zRange(key, offset, offset + limit - 1, { REV: true });
    return this._fetchTasksByIds(taskIds);
  }

  /**
   * Query tasks by queue and status (O(log n))
   */
  static async getTasksByQueueAndStatus(queueName: string, status: TaskStatus, limit: number = 100, offset: number = 0): Promise<Task[]> {
    const client = getRedisClient();
    const key = `${TASK_QUEUE_STATUS_INDEX_PREFIX}${queueName}:status:${status}`;
    const taskIds = await client.zRange(key, offset, offset + limit - 1, { REV: true });
    return this._fetchTasksByIds(taskIds);
  }

  /**
   * Query tasks by creation time (O(log n))
   */
  static async getTasksByCreationTime(startTime: number, endTime: number, limit: number = 100, offset: number = 0): Promise<Task[]> {
    const client = getRedisClient();
    const taskIds = await client.zRange(TASK_INDEX_KEY, startTime, endTime, {
      BY: 'SCORE',
      LIMIT: { offset, count: limit }
    });
    return this._fetchTasksByIds(taskIds);
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
        await this._removeTaskFromIndices(taskId, task.queue, task.status);
        deleted++;
      } else if (!task) {
        // Orphaned index cleanup
        await client.zRem(TASK_INDEX_KEY, taskId);
      }
    }

    logger.info({ deleted, hoursAgo }, 'Old tasks cleaned up');
    return deleted;
  }

  /**
   * Put a task back on its queue for another worker to pick up.
   */
  static async requeueTask(taskId: string): Promise<boolean> {
    const client = getRedisClient();
    const task = await this.getTask(taskId);
    if (!task) {
      return false;
    }

    const previousStatus = task.status;
    task.status = 'queued';
    task.workerId = undefined;
    await client.set(`${TASK_PREFIX}${taskId}`, JSON.stringify(task));

    // Synchronize indices safely
    await this._updateTaskIndices(
      taskId, 
      task.queue, 
      new Date(task.createdAt).getTime(), 
      task.status, 
      previousStatus
    );

    const queueKey = `${QUEUE_PREFIX}${task.queue}`;
    const score = this._calculateQueueScore(task.priority);
    await client.zAdd(queueKey, { score, value: taskId });

    logger.info({ taskId }, 'Task requeued');
    return true;
  }

  /**
   * Recover tasks orphaned by a crashed worker.
   */
  static async recoverStaleTasks(staleMs: number = 5 * 60 * 1000): Promise<number> {
    const client = getRedisClient();
    // Efficiently query only tasks that are 'processing'
    const processingKey = `${TASK_STATUS_INDEX_PREFIX}processing`;
    const taskIds = await client.zRange(processingKey, 0, -1);
    
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
      const previousStatus = task.status;

      task.status = 'queued';
      task.workerId = undefined;
      await client.set(`${TASK_PREFIX}${taskId}`, JSON.stringify(task));

      // Synchronize indices safely
      await this._updateTaskIndices(
        taskId, 
        task.queue, 
        new Date(task.createdAt).getTime(), 
        task.status, 
        previousStatus
      );

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

  // --- Private helper methods ---

  /**
   * Batch resolves an array of IDs into full Task objects efficiently
   */
  private static async _fetchTasksByIds(taskIds: string[]): Promise<Task[]> {
    if (taskIds.length === 0) return [];
    const client = getRedisClient();
    const keys = taskIds.map(id => `${TASK_PREFIX}${id}`);
    const dataList = await client.mGet(keys);

    return dataList
      .filter((data): data is string => data !== null)
      .map((data) => JSON.parse(data));
  }

  /**
   * Synchronize all status and queue search ZSET indices via Redis pipelines
   */
  private static async _updateTaskIndices(
    taskId: string,
    queueName: string,
    timestamp: number,
    newStatus: string,
    oldStatus?: string
  ): Promise<void> {
    const client = getRedisClient();
    const multi = client.multi();

    if (oldStatus && oldStatus !== newStatus) {
      multi.zRem(`${TASK_STATUS_INDEX_PREFIX}${oldStatus}`, taskId);
      multi.zRem(`${TASK_QUEUE_STATUS_INDEX_PREFIX}${queueName}:status:${oldStatus}`, taskId);
    }

    multi.zAdd(`${TASK_STATUS_INDEX_PREFIX}${newStatus}`, { score: timestamp, value: taskId });
    multi.zAdd(`${TASK_QUEUE_STATUS_INDEX_PREFIX}${queueName}:status:${newStatus}`, { score: timestamp, value: taskId });

    await multi.exec();
  }

  /**
   * Purge a task from the search indices entirely
   */
  private static async _removeTaskFromIndices(taskId: string, queueName: string, status: string): Promise<void> {
    const client = getRedisClient();
    const multi = client.multi();
    multi.zRem(`${TASK_STATUS_INDEX_PREFIX}${status}`, taskId);
    multi.zRem(`${TASK_QUEUE_STATUS_INDEX_PREFIX}${queueName}:status:${status}`, taskId);
    await multi.exec();
  }

  private static _calculateQueueScore(priority: string): number {
    const priorityMap: Record<string, number> = {
      critical: 1000,
      high: 100,
      medium: 10,
      low: 1,
    };
    return (priorityMap[priority] || 10) + Math.random();
  }

  private static async _detectDependencyCycle(
    rootId: string,
    rootDeps: string[]
  ): Promise<string[] | null> {
    const visited = new Set<string>();
    const inStack = new Set<string>();
    const path: string[] = [];

    const dfs = async (node: string): Promise<string[] | null> => {
      if (inStack.has(node)) {
        const start = path.indexOf(node);
        return [...path.slice(start), node];
      }
      if (visited.has(node)) {
        return null;
      }

      visited.add(node);
      inStack.add(node);
      path.push(node);

      const deps = node === rootId ? rootDeps : (await this.getTask(node))?.dependencies ?? [];
      for (const dep of deps) {
        const cycle = await dfs(dep);
        if (cycle) {
          return cycle;
        }
      }

      inStack.delete(node);
      path.pop();
      return null;
    };

    return dfs(rootId);
  }

  private static async _checkDependencies(dependencyIds: string[]): Promise<boolean> {
    if (dependencyIds.length === 0) return true;

    const client = getRedisClient();
    const keys = dependencyIds.map(id => `${TASK_PREFIX}${id}`);
    
    // Fetch all dependencies in one network call
    const dataList = await client.mGet(keys);

    for (const data of dataList) {
      if (!data) return false;
      const task: Task = JSON.parse(data);
      if (task.status !== 'completed') {
        return false;
      }
    }
    
    return true;
  }

  private static async _moveToDeadLetterQueue(taskId: string): Promise<void> {
    const client = getRedisClient();
    const task = await this.getTask(taskId);

    if (task) {
      const previousStatus = task.status;
      task.status = 'failed';
      await client.lPush(DEAD_LETTER_QUEUE, JSON.stringify(task));
      await client.del(`${TASK_PREFIX}${taskId}`);
      
      // Clear out the indices completely since this entry is deleted from the primary task space
      await client.zRem(TASK_INDEX_KEY, taskId);
      await this._removeTaskFromIndices(taskId, task.queue, previousStatus);

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
   * Move a task between queue stat counters when its status changes.
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