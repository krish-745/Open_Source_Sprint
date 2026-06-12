import logger from '../utils/logger';
import { Task, TaskStatus } from '../types';
import { TaskQueue } from './task-queue';
import { WorkerPool } from './worker-pool';
import { getRedisClient } from './redis';
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

      // If task is already completed/failed, skip processing to avoid straggler workers overriding state.
      const currentTask = await TaskQueue.getTask(task.id);
      if (currentTask && (currentTask.status === 'completed' || currentTask.status === 'failed')) {
        logger.warn({ taskId: task.id, workerId }, 'Task already finished, skipping execution');
        // Still need to decrement the worker's currentTasks counter.
        await WorkerPool.completeTask(workerId, task.id, {
          duration: Date.now() - startTime,
          success: false,
          retriesUsed: task.retries,
          memory: 0,
          cpu: 0,
        });
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

      let executionResult: any;
      let executionError: Error | undefined;

      try {
        // Execute with timeout, passing a cancellation-aware context.
        const context: TaskExecutionContext = {
          isCancelled: () => this.isCancelled(task.id),
        };
        executionResult = await Promise.race([
          handler(task.payload, context),
          new Promise<never>((_, reject) => {
            timeoutHandle = setTimeout(() => {
              reject(new Error(`Task execution timeout after ${task.timeout}ms`));
            }, task.timeout);
          }),
        ]);
      } catch (err: any) {
        executionError = err;
      } finally {
        if (timeoutHandle) clearTimeout(timeoutHandle);
      }

      // If the task was cancelled mid-flight, record it rather than completing.
      if (this.isCancelled(task.id)) {
        await TaskQueue.updateTaskStatus(task.id, 'cancelled');
        logger.info({ taskId: task.id }, 'Task cancelled during execution');
        
        await WorkerPool.completeTask(workerId, task.id, {
          duration: Date.now() - startTime,
          success: false,
          retriesUsed: task.retries,
          memory: 0,
          cpu: 0,
        });
        return;
      }

      let shouldCompleteTask = true;
      let finalResult = executionResult;
      let finalStatus: TaskStatus = executionError ? 'failed' : 'completed';
      let finalError: string | undefined = executionError ? (executionError.message || String(executionError)) : undefined;

      if (task.consensus && task.consensus.workers > 1) {
        const client = getRedisClient();
        shouldCompleteTask = false;
        
        // Save this worker's result or error
        const resultObj = executionError ? { __consensus_error: finalError } : { __consensus_result: executionResult ?? null };
        await client.hSet(`task:${task.id}:consensus`, workerId, JSON.stringify(resultObj));

        const allResults = await client.hGetAll(`task:${task.id}:consensus`);
        const resultCounts = new Map<string, number>();
        let maxCount = 0;
        let mostFrequentResult: string | null = null;
        
        for (const [wId, res] of Object.entries(allResults)) {
          let weight = 1;
          if (task.consensus.strategy === 'weighted') {
            const workerObj = await WorkerPool.getWorker(wId);
            if (workerObj) {
              const weightTag = workerObj.tags.find(t => t.startsWith('weight:'));
              if (weightTag) {
                weight = Math.max(1, parseInt(weightTag.split(':')[1], 10) || 1);
              }
            }
          }
          
          const count = (resultCounts.get(res) || 0) + weight;
          resultCounts.set(res, count);
          if (count > maxCount) {
            maxCount = count;
            mostFrequentResult = res;
          }
        }

        const totalExpected = task.consensus.workers;
        const totalSubmitted = Object.keys(allResults).length;
        const strategy = task.consensus.strategy || 'majority';

        if (strategy === 'all') {
          if (totalSubmitted >= totalExpected) {
             if (maxCount === totalExpected) {
               shouldCompleteTask = true;
             } else {
               shouldCompleteTask = true;
               mostFrequentResult = JSON.stringify({ __consensus_error: 'Consensus not reached: results did not match for all workers' });
             }
          }
        } else if (strategy === 'majority') {
          const majority = Math.floor(totalExpected / 2) + 1;
          if (maxCount >= majority) {
            shouldCompleteTask = true;
          } else if (totalSubmitted >= totalExpected) {
            shouldCompleteTask = true;
            mostFrequentResult = JSON.stringify({ __consensus_error: 'Consensus not reached: no majority found' });
          }
        } else if (strategy === 'weighted') {
           let totalWeight = 0;
           for (const wId of Object.keys(allResults)) {
             let w = 1;
             const workerObj = await WorkerPool.getWorker(wId);
             if (workerObj) {
                const weightTag = workerObj.tags.find(t => t.startsWith('weight:'));
                if (weightTag) w = Math.max(1, parseInt(weightTag.split(':')[1], 10) || 1);
             }
             totalWeight += w;
           }
           const majorityWeight = (totalWeight / 2);
           if (maxCount > majorityWeight) {
             shouldCompleteTask = true;
           } else if (totalSubmitted >= totalExpected) {
             shouldCompleteTask = true;
             mostFrequentResult = JSON.stringify({ __consensus_error: 'Consensus not reached: no weighted majority found' });
           }
        }

        if (shouldCompleteTask && mostFrequentResult) {
           const parsed = JSON.parse(mostFrequentResult);
           if (parsed.__consensus_error) {
             finalStatus = 'failed';
             finalError = parsed.__consensus_error;
           } else {
             finalStatus = 'completed';
             finalResult = parsed.__consensus_result;
             finalError = undefined;
           }
        }
      }

      const latestTask = await TaskQueue.getTask(task.id);
      
      if (shouldCompleteTask && latestTask && latestTask.status === 'processing') {
        if (finalStatus === 'completed') {
          await TaskQueue.updateTaskStatus(task.id, 'completed', {
            result: finalResult,
            completedAt: new Date(),
          });
          await TaskHooks.emitTask('task.completed', { ...task, status: 'completed', result: finalResult });
          logger.info({ taskId: task.id, duration: Date.now() - startTime }, 'Task completed successfully');
        } else {
          // Non-consensus task failed: attempt retry; exhaust retries → DLQ.
          if (!task.consensus) {
             const retried = await TaskQueue.retryTask(task.id);
             await TaskHooks.emitTask(retried ? 'task.retried' : 'task.failed', {
                ...task,
                status: retried ? 'retry' : 'failed',
                error: finalError,
             });
             if (!retried) {
                await TaskQueue.updateTaskStatus(task.id, 'failed', {
                  error: finalError,
                  completedAt: new Date(),
                });
             }
          } else {
             // Consensus evaluation failed
             await TaskQueue.updateTaskStatus(task.id, 'failed', {
               error: finalError,
               completedAt: new Date(),
             });
             await TaskHooks.emitTask('task.failed', {
                ...task,
                status: 'failed',
                error: finalError,
             });
          }
          logger.error({ taskId: task.id, error: finalError, duration: Date.now() - startTime }, 'Task execution failed');
        }
      } else if (!shouldCompleteTask && executionError && !task.consensus) {
        logger.error({ taskId: task.id, error: finalError }, 'Unexpected state: handler errored without consensus');
      }

      await WorkerPool.completeTask(workerId, task.id, {
        duration: Date.now() - startTime,
        success: finalStatus === 'completed',
        retriesUsed: task.retries,
        memory: 0,
        cpu: 0,
      });

    } catch (error: any) {
      const duration = Date.now() - startTime;
      const errorMessage = error?.message || String(error);
      logger.error({ taskId: task.id, error: errorMessage, duration }, 'Task execution crashed unexpectedly');

      // Attempt to retry the task; if retries are exhausted move it to DLQ.
      try {
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
      } catch {
        // If the task can't be found (e.g. it was already removed), just log.
        logger.warn({ taskId: task.id }, 'Could not retry/fail task after crash');
      }

      await WorkerPool.completeTask(workerId, task.id, {
        duration,
        success: false,
        retriesUsed: task.retries,
        memory: 0,
        cpu: 0,
      });
    } finally {
      this.clearCancellation(task.id);
      const worker = await WorkerPool.getWorker(workerId);
      if (worker) {
        await WorkerPool.updateWorkerStatus(workerId, worker.currentTasks > 0 ? 'busy' : 'idle');
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
}
