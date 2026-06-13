import { v4 as uuidv4 } from 'uuid';
import { WatchError } from 'redis';
import { getRedisClient } from './redis';
import logger, { withTrace } from '../utils/logger';
import { Task, TaskStatus, Queue, RecurrenceRule, ConsensusOptions, TaskBranch } from '../types';

import * as zlib from 'zlib';
import { promisify } from 'util';

const gzip = promisify(zlib.gzip);
const gunzip = promisify(zlib.gunzip);

const TASK_PREFIX = 'task:';
const QUEUE_PREFIX = 'queue:';
const QUEUE_LIST_KEY = 'queues:all';
const TASK_INDEX_KEY = 'tasks:index';
const DEAD_LETTER_QUEUE = 'dlq:tasks';
/** TTL (seconds) for trace-index Redis sets — matches 7-day task retention. */
const TRACE_INDEX_TTL_SECONDS = 7 * 24 * 60 * 60;

const TASK_STATUS_INDEX_PREFIX = 'tasks:status:';
const TASK_QUEUE_STATUS_INDEX_PREFIX = 'tasks:queue:';

// Default time-to-live (seconds) per priority, used when a task is created
// without an explicit ttl. Higher-priority tasks are given longer to start.
const DEFAULT_TTL_BY_PRIORITY: Record<string, number> = {
  critical: 3600,
  high: 1800,
  medium: 900,
  low: 300,
};

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

/**
 * Thrown when a task's cost exceeds the caller's available budget.
 */
export class BudgetExceededError extends Error {
  constructor(callerId: string, available: number, required: number) {
    super(`Budget exceeded for caller ${callerId}: available ${available}, required ${required}`);
    this.name = 'BudgetExceededError';
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
      ttl?: number;
      callbackUrl?: string;
      recurrence?: RecurrenceRule;
      tags?: string[];
      metadata?: Record<string, any>;
      traceId?: string;
      parentSpanId?: string;
      consensus?: ConsensusOptions;
    } = {}
  ): Promise<Task> {
    const client = getRedisClient();
    const taskId = uuidv4();
    const queueName = options.queueName || 'default';
    const priority = options.priority || 'medium';

    const createdAt = new Date();
    const ttl = options.ttl ?? DEFAULT_TTL_BY_PRIORITY[priority];
    const expiresAt = ttl ? new Date(createdAt.getTime() + ttl * 1000) : undefined;

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

    let traceId = options.traceId;
    let parentSpanId = options.parentSpanId;
    if (options.dependencies && options.dependencies.length > 0) {
      // Inherit trace context from the first dependency so the whole chain
      // shares a single traceId and we can reconstruct the call tree.
      const depTask = await this._getRawTask(options.dependencies[0]);
      if (depTask) {
        if (!traceId && depTask.traceId) {
          traceId = depTask.traceId;
        }
        if (!parentSpanId) {
          // The dependency task IS the parent span.
          parentSpanId = depTask.id;
        }
      }
    }
    if (!traceId) {
      traceId = uuidv4();
    }

    // Use ?? (not ||) so that an explicit cost of 0 is honoured as a free task.
    // Clamp to >= 0 and fall back to 1 for non-finite/NaN values so invalid
    // metadata never corrupts budget arithmetic.
    const rawCost = options.metadata?.cost ?? 1;
    const cost = Number.isFinite(rawCost) && rawCost >= 0 ? rawCost : 1;
    const callerId = options.metadata?.callerId;

    if (callerId) {
      const budget = await this.getCallerBudget(callerId);
      if (budget !== null && budget < cost) {
        throw new BudgetExceededError(callerId, budget, cost);
      }
    }
    const task: Task = {
      id: taskId,
      name,
      description: `Task: ${name}`,
      priority,
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
      ttl,
      expiresAt,
      callbackUrl: options.callbackUrl,
      recurrence: options.recurrence,
      tags: options.tags || [],
      metadata: options.metadata || {},
      traceId,
      ...(parentSpanId ? { parentSpanId } : {}),
      consensus: options.consensus,
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

    // Add to trace index (with TTL so sets don't accumulate indefinitely).
    const traceKey = `trace:${traceId}`;
    await client.sAdd(traceKey, taskId);
    await client.expire(traceKey, TRACE_INDEX_TTL_SECONDS);

    // Add to queue
    const score = this._calculateQueueScore(task.priority);
    await client.zAdd(queueKey, { score, value: taskId });

    // Update queue metadata
    await this._updateQueueStats(queueName, 1);

    // Deduct caller budget only after the task is fully persisted, so a
    // write failure never silently drains budget without creating the task.
    if (callerId) {
      const budget = await this.getCallerBudget(callerId);
      if (budget !== null) {
        await client.hIncrBy('caller:budgets', callerId, -cost);
      }
    }

    withTrace({ trace_id: traceId, span_id: taskId, parent_span_id: parentSpanId })
      .info({ taskId, queueName }, 'Task created');
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
   * Get raw task by ID without decompressing result
   */
  private static async _getRawTask(taskId: string): Promise<Task | null> {
    const client = getRedisClient();
    const data = await client.get(`${TASK_PREFIX}${taskId}`);
    return data ? JSON.parse(data) : null;
  }

  /**
   * Get task by ID (transparently decompresses)
   */
  static async getTask(taskId: string): Promise<Task | null> {
    const task = await this._getRawTask(taskId);
    if (!task) return null;
    return await this._decompressTask(task);
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
    const task = await this._getRawTask(taskId);

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
      try {
        await client.executeIsolated(async (isolatedClient) => {
          await isolatedClient.watch(key);

          const data = await isolatedClient.get(key);
          if (!data) {
            await isolatedClient.unwatch();
            throw new Error(`Task ${taskId} not found`);
          }

          const task: Task = JSON.parse(data);
          const previousStatus = task.status;

          task.status = status;
          if (metadata) {
            const metaCopy = { ...metadata };
            if (metaCopy.result !== undefined) {
              metaCopy.result = await this._compressPayload(metaCopy.result, task.queue, taskId);
            }
            Object.assign(task, metaCopy);
          }
          if (status === 'completed') {
            task.completedAt = new Date();
          } else if (status === 'processing') {
            task.startedAt = new Date();
          }

          const timestamp = new Date(task.createdAt).getTime();
          const multi = isolatedClient.multi().set(key, JSON.stringify(task));

          // Incorporate index updates directly into the optimistic locking transaction
          if (previousStatus !== status) {
            multi.zRem(`${TASK_STATUS_INDEX_PREFIX}${previousStatus}`, taskId);
            multi.zRem(`${TASK_QUEUE_STATUS_INDEX_PREFIX}${task.queue}:status:${previousStatus}`, taskId);
            multi.zAdd(`${TASK_STATUS_INDEX_PREFIX}${status}`, { score: timestamp, value: taskId });
            multi.zAdd(`${TASK_QUEUE_STATUS_INDEX_PREFIX}${task.queue}:status:${status}`, { score: timestamp, value: taskId });
          }

          const replies = await multi.exec();
          if (replies === null) {
            throw new WatchError();
          }

          // Keep queue stats consistent as the task moves between states.
          if (previousStatus !== status) {
            await this._transitionQueueStats(task.queue, previousStatus, status);
          }

          if (task.traceId) {
            withTrace({ trace_id: task.traceId, span_id: taskId, parent_span_id: task.parentSpanId })
              .info({ taskId, status }, 'Task status updated');
          } else {
            logger.info({ taskId, status }, 'Task status updated');
          }
        });

        return;
      } catch (error) {
        if (error instanceof WatchError || (error as Error).message.includes('watched keys has been changed')) {
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

        // Try to acquire the task atomically
        if (task.consensus) {
          const dispatchKey = `${QUEUE_PREFIX}${queueName}:dispatch`;
          const count = await client.hIncrBy(dispatchKey, taskId, 1);
          if (count <= task.consensus.workers) {
            if (count === task.consensus.workers) {
              // Last worker needed for consensus has picked it up; remove from queue
              await client.zRem(queueKey, taskId);
            }
            return task;
          }
        } else {
          const removed = await client.zRem(queueKey, taskId);
          if (removed === 1) {
            return task;
          }
        }
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

        // Try to acquire the task atomically
        if (task.consensus) {
          const dispatchKey = `${QUEUE_PREFIX}${queueName}:dispatch`;
          const count = await client.hIncrBy(dispatchKey, taskId, 1);
          if (count <= task.consensus.workers) {
            if (count === task.consensus.workers) {
              await client.zRem(queueKey, taskId);
            }
            batch.push(task);
          }
        } else {
          const removed = await client.zRem(queueKey, taskId);
          if (removed === 1) {
            batch.push(task);
          }
        }
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
    const task = await this._getRawTask(taskId);

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
    await client.hDel(`${queueKey}:dispatch`, taskId);
    await client.del(`task:${taskId}:consensus`);

    if (task.traceId) {
      withTrace({ trace_id: task.traceId, span_id: taskId, parent_span_id: task.parentSpanId })
        .info({ taskId, attempt: task.retries }, 'Task retry queued');
    } else {
      logger.info({ taskId, attempt: task.retries }, 'Task retry queued');
    }
    return true;
  }

  /**
   * Get tasks from queue, with optional filters
   * Cancel a task.
   */
  static async cancelTask(taskId: string): Promise<boolean> {
    const client = getRedisClient();
    const task = await this._getRawTask(taskId);

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
    await client.hDel(`${QUEUE_PREFIX}${task.queue}:dispatch`, taskId);
    await client.del(`task:${taskId}:consensus`);

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
  static async getQueueTasks(
    queueName: string,
    limit: number = 100,
    offset: number = 0,
    filters: {
      status?: string | string[];
      priority?: string | string[];
      tags?: string | string[];
      startDate?: Date;
      endDate?: Date;
    } = {}
  ): Promise<Task[]> {
    const client = getRedisClient();
    const queueKey = `${QUEUE_PREFIX}${queueName}`;

    const hasFilters =
      filters.status ||
      filters.priority ||
      filters.tags ||
      filters.startDate ||
      filters.endDate;

    if (!hasFilters) {
      const taskIds = await client.zRange(queueKey, offset, offset + limit - 1, { REV: true });
      const tasks: Task[] = [];

      for (const taskId of taskIds) {
        const task = await this.getTask(taskId);
        if (task) tasks.push(task);
      }

      return tasks;
    }

    // Retrieve all tasks to filter them in memory
    const allTaskIds = await client.zRange(queueKey, 0, -1, { REV: true });
    const filteredTasks: Task[] = [];

    for (const taskId of allTaskIds) {
      const task = await this.getTask(taskId);
      if (!task) continue;

      // Filter by status (AND logic)
      if (filters.status) {
        const statuses = Array.isArray(filters.status) ? filters.status : [filters.status];
        if (!statuses.includes(task.status)) continue;
      }

      // Filter by priority (AND logic)
      if (filters.priority) {
        const priorities = Array.isArray(filters.priority) ? filters.priority : [filters.priority];
        if (!priorities.includes(task.priority)) continue;
      }

      // Filter by tags (AND logic: must match all specified tags)
      if (filters.tags) {
        const requiredTags = Array.isArray(filters.tags) ? filters.tags : [filters.tags];
        const hasAllTags = requiredTags.every(tag => task.tags.includes(tag));
        if (!hasAllTags) continue;
      }

      // Filter by date range (AND logic)
      if (filters.startDate) {
        if (new Date(task.createdAt) < new Date(filters.startDate)) continue;
      }
      if (filters.endDate) {
        if (new Date(task.createdAt) > new Date(filters.endDate)) continue;
      }

      filteredTasks.push(task);
    }

    // Apply offset and limit
    return filteredTasks.slice(offset, offset + limit);
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
   * Get all tasks by trace ID
   */
  static async getTasksByTraceId(traceId: string): Promise<Task[]> {
    const client = getRedisClient();
    const taskIds = await client.sMembers(`trace:${traceId}`);
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
      rateLimitViolations: parseInt(stats.rateLimitViolations || '0'),
      totalCompressedTasks: parseInt(stats.totalCompressedTasks || '0'),
      compressedBytesSaved: parseInt(stats.compressedBytesSaved || '0'),
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
      const task = await this._getRawTask(taskId);
      if (task && (task.status === 'completed' || task.status === 'failed' || task.status === 'cancelled')) {
        await client.del(`${TASK_PREFIX}${taskId}`);
        await client.zRem(TASK_INDEX_KEY, taskId);
        await client.hDel(`${QUEUE_PREFIX}${task.queue}:dispatch`, taskId);
        await client.del(`task:${taskId}:consensus`);
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
   * Whether a task has passed its expiry and has not started running yet.
   * Only un-started tasks (pending/queued/blocked/retry) can expire.
   */
  static isExpired(task: Task, now: number = Date.now()): boolean {
    if (!task.expiresAt) {
      return false;
    }
    const notStarted = ['pending', 'queued', 'blocked', 'retry'].includes(task.status);
    return notStarted && new Date(task.expiresAt).getTime() <= now;
  }

  /**
   * Cancel tasks that expired before being picked up, removing them from their
   * queue and marking them `cancelled`. Returns the number of expired tasks.
   */
  static async expireStaleTasks(): Promise<number> {
    const client = getRedisClient();
    const taskIds = await client.zRange(TASK_INDEX_KEY, 0, -1);
    const now = Date.now();
    let expired = 0;

    for (const taskId of taskIds) {
      const task = await this._getRawTask(taskId);
      if (!task || !this.isExpired(task, now)) {
        continue;
      }

      task.status = 'cancelled';
      await client.set(`${TASK_PREFIX}${taskId}`, JSON.stringify(task));
      await client.zRem(`${QUEUE_PREFIX}${task.queue}`, taskId);
      expired++;
      logger.warn({ taskId, expiresAt: task.expiresAt }, 'Task expired before execution');
    }

    if (expired > 0) {
      logger.info({ expired }, 'Expired tasks cancelled');
    }
    return expired;
  }

  /**
   * Search tasks by name and description.
   * Compares the search term case-insensitively using simple string inclusion to handle special characters safely.
   * Returns matching task IDs along with a relevance score (exact name match = 10, partial name match = 5, description match = 2).
   */
  static async searchTasks(
    searchTerm: string,
    limit: number = 50
  ): Promise<{ taskId: string; score: number }[]> {
    const client = getRedisClient();
    const taskIds = await client.zRange(TASK_INDEX_KEY, 0, -1);

    const results: { taskId: string; score: number }[] = [];
    const term = searchTerm.toLowerCase().trim();
    if (!term) return [];

    for (const taskId of taskIds) {
      const task = await this.getTask(taskId);
      if (!task) continue;

      const name = (task.name || '').toLowerCase();
      const description = (task.description || '').toLowerCase();

      let score = 0;

      // Check name match
      if (name === term) {
        score += 10;
      } else if (name.includes(term)) {
        score += 5;
      }

      // Check description match
      if (description.includes(term)) {
        score += 2;
      }

      if (score > 0) {
        results.push({ taskId, score });
      }
    }

    // Sort by relevance score (highest first)
    results.sort((a, b) => b.score - a.score);

    // Limit results to prevent performance issues
    return results.slice(0, limit);
  }

  /**
   * Put a task back on its queue for another worker to pick up, clearing any
   * previous worker assignment. Used when a worker disconnects mid-execution.
   * Returns false if the task no longer exists.
   */
  static async requeueTask(taskId: string): Promise<boolean> {
    const client = getRedisClient();
    const task = await this._getRawTask(taskId);
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
    await client.hDel(`${queueKey}:dispatch`, taskId);

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
      const task = await this._getRawTask(taskId);
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
      await client.hDel(`${queueKey}:dispatch`, taskId);

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

  /**
  /**
   * Configure rate limit for a queue (max tasks per second)
   */
  static async configureRateLimit(queueName: string, maxTasksPerSecond: number): Promise<void> {
    const client = getRedisClient();
    const configKey = `${QUEUE_PREFIX}${queueName}:config`;
    await client.hSet(configKey, 'rateLimit', maxTasksPerSecond.toString());
  }

  /**
   * Get the configured rate limit for a queue. Returns null if not configured.
   */
  static async getRateLimit(queueName: string): Promise<number | null> {
    const client = getRedisClient();
    const configKey = `${QUEUE_PREFIX}${queueName}:config`;
    const rateLimitStr = await client.hGet(configKey, 'rateLimit');
    if (!rateLimitStr) return null;
    const limit = parseInt(rateLimitStr, 10);
    return isNaN(limit) ? null : limit;
  }

  /**
   * Consume a token from the token bucket for the queue. Returns true if allowed.
   */
  static async consumeRateLimitToken(queueName: string, workerId?: string): Promise<boolean> {
    if (!queueName) return true; // Guard against empty queue name

    const client = getRedisClient();
    const configKey = `${QUEUE_PREFIX}${queueName}:config`;
    const rateLimitStr = await client.hGet(configKey, 'rateLimit');
    
    if (!rateLimitStr) return true; // No rate limit configured
    
    const limit = parseInt(rateLimitStr, 10);
    if (limit <= 0) return false;
    
    // Lua script: atomically compute token bucket and consume one token if available.
    // On denial, does NOT update the timestamp so the refill window is not artificially advanced.
    const luaScript = `
      local tokens_key = KEYS[1]
      local timestamp_key = KEYS[2]
      local rate = tonumber(ARGV[1])
      local capacity = tonumber(ARGV[2])
      local now = tonumber(ARGV[3])
      local ttl = tonumber(ARGV[4])
      
      local tokens = tonumber(redis.call('get', tokens_key))
      if tokens == nil then
        tokens = capacity
      end
      
      local last_refill = tonumber(redis.call('get', timestamp_key))
      if last_refill == nil then
        last_refill = now
      end
      
      local delta = math.max(0, now - last_refill)
      local refill = (delta / 1000) * rate
      tokens = math.min(capacity, tokens + refill)
      
      if tokens >= 1 then
        tokens = tokens - 1
        redis.call('set', tokens_key, tokens)
        redis.call('expire', tokens_key, ttl)
        redis.call('set', timestamp_key, now)
        redis.call('expire', timestamp_key, ttl)
        return 1
      else
        -- On denial, DO NOT persist the partially refilled token count.
        -- Just refresh the TTL so the keys don't expire prematurely.
        redis.call('expire', tokens_key, ttl)
        redis.call('expire', timestamp_key, ttl)
        return 0
      end
    `;
    
    // Per worker/queue combination
    const tokensKey = workerId 
      ? `${QUEUE_PREFIX}${queueName}:worker:${workerId}:tokens`
      : `${QUEUE_PREFIX}${queueName}:tokens`;
      
    const timestampKey = workerId 
      ? `${QUEUE_PREFIX}${queueName}:worker:${workerId}:last_refill`
      : `${QUEUE_PREFIX}${queueName}:last_refill`;
    
    // TTL = 2x the replenish window (1 second per token at 1 task/sec rate, use 60s safety margin)
    const ttlSeconds = Math.max(60, Math.ceil(limit * 2));

    const result = await client.eval(luaScript, {
      keys: [tokensKey, timestampKey],
      arguments: [limit.toString(), limit.toString(), Date.now().toString(), ttlSeconds.toString()]
    });
    
    if (result === 1) {
      return true;
    } else {
      const statsKey = `${QUEUE_PREFIX}${queueName}:stats`;
      await client.hIncrBy(statsKey, 'rateLimitViolations', 1);
      return false;
    }
  }

  /** Set budget for a caller
   */
  static async setCallerBudget(callerId: string, budget: number): Promise<void> {
    const client = getRedisClient();
    await client.hSet('caller:budgets', callerId, budget.toString());
  }

  /**
   * Get budget for a caller
   */
  static async getCallerBudget(callerId: string): Promise<number | null> {
    const client = getRedisClient();
    const val = await client.hGet('caller:budgets', callerId);
    return val ? parseInt(val, 10) : null;
  }

  /**
   * Predict the total cost of all pending tasks in a queue
   */
  static async predictQueueCost(queueName: string): Promise<{ totalCost: number, taskCount: number }> {
    const client = getRedisClient();
    const queueKey = `${QUEUE_PREFIX}${queueName}`;
    const taskIds = await client.zRange(queueKey, 0, -1);
    
    if (taskIds.length === 0) return { totalCost: 0, taskCount: 0 };

    let totalCost = 0;
    for (const id of taskIds) {
      const data = await client.get(`${TASK_PREFIX}${id}`);
      if (data) {
        const t: Task = JSON.parse(data);
        const rawCost = t.metadata?.cost ?? 1;
        const cost = Number.isFinite(rawCost) && rawCost >= 0 ? rawCost : 1;
        totalCost += cost;
      }
    }
    return { totalCost, taskCount: taskIds.length };
  }

  // --- Private helper methods ---

  /**
   * Batch resolves an array of IDs into full Task objects efficiently
   */
  private static async _fetchTasksByIds(taskIds: string[]): Promise<Task[]> {
    if (!taskIds.length) return [];
    const client = getRedisClient();
    const keys = taskIds.map(id => `${TASK_PREFIX}${id}`);
    
    // Use mGet to fetch all task payloads in one round-trip
    const payloads = await client.mGet(keys);
    
    const tasks = payloads
      .filter((data): data is string => data !== null)
      .map(data => JSON.parse(data) as Task);
      
    return Promise.all(tasks.map(task => this._decompressTask(task)));
  }

  /**
   * Synchronize all status and queue search ZSET indices via Redis pipelines
   */
  private static async _updateTaskIndices(
    taskId: string,
    queueName: string,
    timestamp: number,
    newStatus: Task['status'],
    oldStatus?: Task['status']
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

      const deps = node === rootId ? rootDeps : (await this._getRawTask(node))?.dependencies ?? [];
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
    const task = await this._getRawTask(taskId);

    if (task) {
      const previousStatus = task.status;
      task.status = 'failed';
      await client.lPush(DEAD_LETTER_QUEUE, JSON.stringify(task));
      await client.del(`${TASK_PREFIX}${taskId}`);
      await client.hDel(`${QUEUE_PREFIX}${task.queue}:dispatch`, taskId);
      await client.del(`task:${taskId}:consensus`);
      
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
  private static async _compressPayload(result: any, queueName: string, taskId: string): Promise<any> {
    if (result === undefined || result === null) return result;

    const resultString = JSON.stringify(result);
    const originalByteLength = Buffer.byteLength(resultString, 'utf8');

    // Force compression if the user payload looks like our internal prefixes to prevent spoofing bypass
    const isSpoofed = typeof result === 'string' && (result.startsWith('__gz_json_b64__:') || result.startsWith('__gz_b64__:'));

    if (originalByteLength > 10 * 1024 || isSpoofed) { // > 10KB
      try {
        const compressed = await gzip(resultString);
        
        const ratio = ((compressed.length / originalByteLength) * 100).toFixed(2);
        logger.info({ 
          taskId, 
          originalSize: originalByteLength, 
          compressedSize: compressed.length, 
          ratio: `${ratio}%` 
        }, 'Task result compressed');

        const client = getRedisClient();
        const statsKey = `${QUEUE_PREFIX}${queueName}:stats`;
        await client.hIncrBy(statsKey, 'totalCompressedTasks', 1);
        await client.hIncrBy(statsKey, 'compressedBytesSaved', originalByteLength - compressed.length);

        return `__gz_json_b64__:${compressed.toString('base64')}`;
      } catch (err) {
        logger.error({ taskId, error: err }, 'Failed to compress task result');
      }
    }
    
    return result;
  }

  private static async _decompressTask(task: Task): Promise<Task> {
    if (task.result && typeof task.result === 'string') {
      try {
        if (task.result.startsWith('__gz_json_b64__:')) {
          const base64Data = task.result.substring('__gz_json_b64__:'.length);
          const compressed = Buffer.from(base64Data, 'base64');
          // Apply maxOutputLength to prevent zip bomb memory exhaustion
          const decompressed = await gunzip(compressed, { maxOutputLength: 50 * 1024 * 1024 });
          return {
            ...task,
            result: JSON.parse(decompressed.toString('utf-8'))
          };
        } else if (task.result.startsWith('__gz_b64__:')) {
          const base64Data = task.result.substring('__gz_b64__:'.length);
          const compressed = Buffer.from(base64Data, 'base64');
          const decompressed = await gunzip(compressed, { maxOutputLength: 50 * 1024 * 1024 });
          const decompressedString = decompressed.toString('utf-8');
          
          let parsedResult = decompressedString;
          try {
            const parsed = JSON.parse(decompressedString);
            if (typeof parsed === 'object' && parsed !== null) {
              parsedResult = parsed;
            }
          } catch (e) {
            // Primitive or unparseable fallback -> retain as raw string to avoid morphing
          }
          return {
            ...task,
            result: parsedResult
          };
        }
      } catch (err) {
        logger.error({ taskId: task.id, error: err }, 'Failed to decompress task result');
      }
    }
    return task;
  }
}
