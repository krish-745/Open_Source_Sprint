import logger from '../utils/logger';
import { Task, TaskStatus } from '../types';
import { TaskQueue } from './task-queue';
import { WorkerPool } from './worker-pool';
import { deliverCallback } from './task-callbacks';
import { TaskHooks } from './task-hooks';

export interface TaskExecutionContext {
  /** Handlers can poll this to cooperatively stop work when cancelled. */
  isCancelled: () => boolean;
}

export interface TaskHandler {
  (payload: Record<string, any>, context?: TaskExecutionContext): Promise<any>;
}

export class TaskExecutor {
  private static handlers: Map<string, TaskHandler> = new Map();
  private static cancelledTasks: Set<string> = new Set();

  /**
   * Request cancellation of a task. Removes it from the queue side via
   * TaskQueue.cancelTask; here we record the signal so a running handler can
   * observe it and the executor skips it if not yet started.
   */
  static cancel(taskId: string): void {
    this.cancelledTasks.add(taskId);
  }

  static isCancelled(taskId: string): boolean {
    return this.cancelledTasks.has(taskId);
  }

  static clearCancellation(taskId: string): void {
    this.cancelledTasks.delete(taskId);
  }

  /**
   * Register a task handler
   */
  static registerHandler(name: string, handler: TaskHandler): void {
    this.handlers.set(name, handler);
    logger.info({ handlerName: name }, 'Task handler registered');
  }

  /**
   * Execute a task
   */
  static async execute(workerId: string, task: Task): Promise<void> {
    const startTime = Date.now();
    let timeoutHandle: NodeJS.Timeout | undefined;

    try {
      // Skip tasks cancelled before they start.
      if (this.isCancelled(task.id)) {
        await TaskQueue.updateTaskStatus(task.id, 'cancelled');
        logger.info({ taskId: task.id }, 'Task cancelled before execution');
        return;
      }

      // Validate handler exists
      const handler = this.handlers.get(task.handler);
      if (!handler) {
        throw new Error(`No handler registered for: ${task.handler}`);
      }

      // Validate timeout
      if (task.timeout <= 0) {
        throw new Error('Task timeout must be positive');
      }

      // Update task status
      await TaskQueue.updateTaskStatus(task.id, 'processing', {
        workerId,
        startedAt: new Date(),
      });

      await WorkerPool.updateWorkerStatus(workerId, 'busy');
      await TaskHooks.emitTask('task.started', task);

      // Execute with timeout, passing a cancellation-aware context.
      const context: TaskExecutionContext = {
        isCancelled: () => this.isCancelled(task.id),
      };
      const result = await Promise.race([
        handler(task.payload, context),
        new Promise<never>((_, reject) => {
          timeoutHandle = setTimeout(() => {
            reject(new Error(`Task execution timeout after ${task.timeout}ms`));
          }, task.timeout);
        }),
      ]);

      // If the task was cancelled mid-flight, record it rather than completing.
      if (this.isCancelled(task.id)) {
        await TaskQueue.updateTaskStatus(task.id, 'cancelled');
        logger.info({ taskId: task.id }, 'Task cancelled during execution');
        return;
      }

      // Mark as completed
      await TaskQueue.updateTaskStatus(task.id, 'completed', {
        result,
        completedAt: new Date(),
      });

      const duration = Date.now() - startTime;
      await WorkerPool.completeTask(workerId, task.id, {
        duration,
        success: true,
        retriesUsed: task.retries,
        memory: 0,
        cpu: 0,
      });

      await TaskHooks.emitTask('task.completed', { ...task, status: 'completed', result });

      // Best-effort: POST the result to the task's callback URL if set.
      if (task.callbackUrl) {
        await deliverCallback({ ...task, status: 'completed', completedAt: new Date() }, result);
      }

      logger.info({ taskId: task.id, duration }, 'Task completed successfully');
    } catch (error: any) {
      const duration = Date.now() - startTime;
      const errorMessage = error?.message || String(error);

      logger.error({ taskId: task.id, error: errorMessage, duration }, 'Task execution failed');

      // Attempt retry
      const retried = await TaskQueue.retryTask(task.id);
      await TaskHooks.emitTask(retried ? 'task.retried' : 'task.failed', {
        ...task,
        status: retried ? 'retry' : 'failed',
        error: errorMessage,
      });

      if (retried) {
        await WorkerPool.completeTask(workerId, task.id, {
          duration,
          success: false,
          retriesUsed: task.retries,
          memory: 0,
          cpu: 0,
        });
      } else {
        // Move to dead letter queue
        await TaskQueue.updateTaskStatus(task.id, 'failed', {
          error: errorMessage,
          completedAt: new Date(),
        });

        await WorkerPool.completeTask(workerId, task.id, {
          duration,
          success: false,
          retriesUsed: task.retries,
          memory: 0,
          cpu: 0,
        });
      }
    } finally {
      if (timeoutHandle) clearTimeout(timeoutHandle);
      this.clearCancellation(task.id);

      // Reflect the worker's actual state: it is only idle once it has no
      // remaining tasks. With concurrent execution, other tasks may still be
      // running when this one finishes, so the worker should stay busy.
      const worker = await WorkerPool.getWorker(workerId);
      if (worker) {
        await WorkerPool.updateWorkerStatus(workerId, worker.currentTasks > 0 ? 'busy' : 'idle');
      }
    }
  }

  /**
   * Execute a task with Multi-Quorum Consensus
   */
  static async executeWithConsensus(workerIds: string[], task: Task): Promise<void> {
    const startTime = Date.now();
    let timeoutHandle: NodeJS.Timeout | undefined;

    try {
      if (this.isCancelled(task.id)) {
        await TaskQueue.updateTaskStatus(task.id, 'cancelled');
        logger.info({ taskId: task.id }, 'Task cancelled before execution');
        return;
      }

      const handler = this.handlers.get(task.handler);
      if (!handler) {
        throw new Error(`No handler registered for: ${task.handler}`);
      }
      if (task.timeout <= 0) {
        throw new Error('Task timeout must be positive');
      }
      if (!task.quorum) {
        throw new Error('Task does not specify a quorum');
      }

      await TaskQueue.updateTaskStatus(task.id, 'processing', {
        startedAt: new Date(),
      });

      for (const workerId of workerIds) {
        await WorkerPool.updateWorkerStatus(workerId, 'busy');
      }
      await TaskHooks.emitTask('task.started', task);

      const context: TaskExecutionContext = {
        isCancelled: () => this.isCancelled(task.id),
      };

      const executionPromises = workerIds.map(async (workerId) => {
        const workerStartTime = Date.now();
        try {
          const result = await Promise.race([
            handler(task.payload, context),
            new Promise<never>((_, reject) => {
              if (!timeoutHandle) {
                timeoutHandle = setTimeout(() => {
                  reject(new Error(`Task execution timeout after ${task.timeout}ms`));
                }, task.timeout);
              }
            }),
          ]);
          return { workerId, success: true, result, duration: Date.now() - workerStartTime };
        } catch (error) {
          return { workerId, success: false, error, duration: Date.now() - workerStartTime };
        }
      });

      const workerResults = await Promise.all(executionPromises);

      if (this.isCancelled(task.id)) {
        await TaskQueue.updateTaskStatus(task.id, 'cancelled');
        logger.info({ taskId: task.id }, 'Task cancelled during execution');
        return;
      }

      const resultGroups = new Map<string, { result: any; workerIds: string[] }>();
      
      for (const r of workerResults) {
        if (!r.success) continue;
        
        const hash = this._hashResult(r.result);
        if (!resultGroups.has(hash)) {
          resultGroups.set(hash, { result: r.result, workerIds: [] });
        }
        resultGroups.get(hash)!.workerIds.push(r.workerId);
      }

      let winningGroup: { result: any; workerIds: string[] } | undefined = undefined;
      const quorumCount = task.quorum.count;
      const strategy = task.quorum.strategy;

      for (const group of resultGroups.values()) {
        if (strategy === 'all') {
          if (group.workerIds.length === quorumCount && workerResults.every((r) => r.success)) {
            winningGroup = group;
            break;
          }
        } else if (strategy === 'majority' || strategy === 'weighted') {
          if (group.workerIds.length > quorumCount / 2) {
            winningGroup = group;
            break;
          }
        }
      }

      if (winningGroup) {
        await TaskQueue.updateTaskStatus(task.id, 'completed', {
          result: winningGroup.result,
          completedAt: new Date(),
        });

        for (const r of workerResults) {
          const workerForCost = await WorkerPool.getWorker(r.workerId);
          const costIncurred = workerForCost && (WorkerPool as any).calculateTaskCost ? (WorkerPool as any).calculateTaskCost(workerForCost, task) : 0;
          const isWinner = winningGroup.workerIds.includes(r.workerId);
          
          await WorkerPool.completeTask(r.workerId, task.id, {
            duration: r.duration,
            success: isWinner,
            retriesUsed: task.retries,
            memory: 0,
            cpu: 0,
            ...costIncurred ? { costIncurred } : {}
          });
        }

        const duration = Date.now() - startTime;
        await TaskHooks.emitTask('task.completed', { ...task, status: 'completed', result: winningGroup.result });

        if (task.callbackUrl) {
          await deliverCallback({ ...task, status: 'completed', completedAt: new Date() }, winningGroup.result);
        }

        logger.info({ taskId: task.id, duration, winningWorkers: winningGroup.workerIds.length }, 'Task reached consensus');
      } else {
        throw new Error('Consensus not reached');
      }
    } catch (error: any) {
      const duration = Date.now() - startTime;
      const errorMessage = error?.message || String(error);

      logger.error({ taskId: task.id, error: errorMessage, duration }, 'Task consensus execution failed');

      const retried = await TaskQueue.retryTask(task.id);
      await TaskHooks.emitTask(retried ? 'task.retried' : 'task.failed', {
        ...task,
        status: retried ? 'retry' : 'failed',
        error: errorMessage,
      });

      if (!retried) {
        await TaskQueue.updateTaskStatus(task.id, 'failed', {
          error: errorMessage,
          completedAt: new Date(),
        });
      }

      for (const workerId of workerIds) {
        const workerForCost = await WorkerPool.getWorker(workerId);
        const costIncurred = workerForCost && (WorkerPool as any).calculateTaskCost ? (WorkerPool as any).calculateTaskCost(workerForCost, task) : 0;

        await WorkerPool.completeTask(workerId, task.id, {
          duration,
          success: false,
          retriesUsed: task.retries,
          memory: 0,
          cpu: 0,
          ...costIncurred ? { costIncurred } : {}
        });
      }
    } finally {
      if (timeoutHandle) clearTimeout(timeoutHandle);
      this.clearCancellation(task.id);

      for (const workerId of workerIds) {
        const worker = await WorkerPool.getWorker(workerId);
        if (worker) {
          await WorkerPool.updateWorkerStatus(workerId, worker.currentTasks > 0 ? 'busy' : 'idle');
        }
      }
    }
  }

  /**
   * Execute a batch of tasks on a worker.
   *
   * Tasks run concurrently; each goes through the normal single-task lifecycle,
   * so one task failing does not abort the others (partial-batch failure
   * handling). Returns a summary with per-batch performance metrics.
   */
  static async executeBatch(
    workerId: string,
    tasks: Task[]
  ): Promise<{ total: number; succeeded: number; failed: number; durationMs: number }> {
    const start = Date.now();
    const results = await Promise.allSettled(tasks.map((task) => this.execute(workerId, task)));

    const failed = results.filter((r) => r.status === 'rejected').length;
    const succeeded = results.length - failed;
    const durationMs = Date.now() - start;

    logger.info({ workerId, total: tasks.length, succeeded, failed, durationMs }, 'Batch executed');
    return { total: tasks.length, succeeded, failed, durationMs };
  }

  /**
   * Get all registered handlers
   */
  static getRegisteredHandlers(): string[] {
    return Array.from(this.handlers.keys());
  }

  /**
   * Check if handler is registered
   */
  static hasHandler(name: string): boolean {
    return this.handlers.has(name);
  }

  /**
   * Clear all handlers (useful for testing)
   */
  static clearHandlers(): void {
    this.handlers.clear();
  }

  // Private helper methods

  private static _timeoutPromise(timeoutMs: number): Promise<never> {
    return new Promise((_, reject) => {
      setTimeout(() => {
        reject(new Error(`Task execution timeout after ${timeoutMs}ms`));
      }, timeoutMs);
    });
  }

  private static _hashResult(result: any): string {
    if (result === undefined) return 'undefined';
    if (result === null) return 'null';
    if (typeof result !== 'object') return String(result);

    if (Array.isArray(result)) {
      return JSON.stringify(result.map((item) => this._hashResult(item)));
    }
    
    // Sort keys to make object hashing deterministic
    const sortedKeys = Object.keys(result).sort();
    const sortedObj: any = {};
    for (const key of sortedKeys) {
      sortedObj[key] = result[key];
    }
    return JSON.stringify(sortedObj);
  }
}
