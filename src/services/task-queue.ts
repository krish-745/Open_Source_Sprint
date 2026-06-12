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
      createdAt: new Date(),
      queue: queueName,
      dependencies: options.dependencies || [],
      scheduledFor: options.scheduledFor,
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
   * Evaluate a task's conditional branches against its result and return the
   * branches that match, so the caller can enqueue the next step(s). A
   * condition prefixed with `regex:` is matched as a regular expression against
   * the stringified result; otherwise a substring match is used. Multiple
   * branches may match.
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
   * Update task status atomically.
   *
   * Concurrent workers can update the same task at once. To avoid lost updates
   * from a non-atomic read-modify-write, this uses Redis optimistic locking
   * (WATCH/MULTI/EXEC): the task key is watched, mutated, and committed in a
   * transaction. If another client modifies the key first, the transaction is
   * aborted (WatchError) and the operation is retried on a fresh read.
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
        await client.multi().set(key, JSON.stringify(task)).exec();

        // Keep queue stats consistent as the task moves between states.
        if (previousStatus !== status) {
          await this._transitionQueueStats(task.queue, previousStatus, status);
        }

        logger.info({ taskId, status }, 'Task status updated');
        return;
      } catch (error) {
        if (error instanceof WatchError) {
          // The task changed under us; retry with the latest value.
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
   * Fetch up to `batchSize` runnable tasks from a queue in priority order, for
   * workers that process tasks in batches. Applies the same runnability rules
   * as getNextTask (dependencies met, not scheduled for later).
   */
  static async getNextBatch(queueName: string, batchSize: number = 10): Promise<Task[]> {
    if (batchSize < 1) {
      return [];
    }
    const client = getRedisClient();
    const queueKey = `${QUEUE_PREFIX}${queueName}`;
    const taskIds = await client.zRange(queueKey, 0, -1, { REV: true });

    const batch: Task[] = [];
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
   * Cancel a task. Terminal tasks (completed/failed/already cancelled) cannot
   * be cancelled and return false. Otherwise the task is marked `cancelled` and
   * removed from its queue so it won't be dispatched. Returns true on success.
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

    task.status = 'cancelled';
    await client.set(`${TASK_PREFIX}${taskId}`, JSON.stringify(task));
    await client.zRem(`${QUEUE_PREFIX}${task.queue}`, taskId);

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
   * Put a task back on its queue for another worker to pick up, clearing any
   * previous worker assignment. Used when a worker disconnects mid-execution.
   * Returns false if the task no longer exists.
   */
  static async requeueTask(taskId: string): Promise<boolean> {
    const client = getRedisClient();
    const task = await this.getTask(taskId);
    if (!task) {
      return false;
    }

    task.status = 'queued';
    task.workerId = undefined;
    await client.set(`${TASK_PREFIX}${taskId}`, JSON.stringify(task));

    const queueKey = `${QUEUE_PREFIX}${task.queue}`;
    const score = this._calculateQueueScore(task.priority);
    await client.zAdd(queueKey, { score, value: taskId });

    logger.info({ taskId }, 'Task requeued');
    return true;
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

  /**
   * Detect a circular dependency reachable from a new task using depth-first
   * search. The new task (`rootId`) is treated as a graph node whose edges are
   * its declared `rootDeps`; every other node's edges are read from the stored
   * task's `dependencies`. Returns the cycle as an ordered list of task ids, or
   * `null` if the dependency graph is acyclic. Transitive dependencies are
   * followed, and missing/unknown dependencies are treated as leaf nodes.
   */
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
