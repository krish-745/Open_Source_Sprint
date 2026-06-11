import logger from '../utils/logger';
import { Task, TaskStatus } from '../types';
import { TaskQueue } from './task-queue';
import { WorkerPool } from './worker-pool';
import { getRedisClient } from './redis';

export interface TaskHandler {
  (payload: Record<string, any>): Promise<any>;
}

export class TaskExecutor {
  private static handlers: Map<string, TaskHandler> = new Map();

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

      let executionResult: any;
      let executionError: Error | undefined;

      try {
        // Execute with timeout
        executionResult = await Promise.race([
          handler(task.payload),
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
          logger.info({ taskId: task.id, duration: Date.now() - startTime }, 'Task completed successfully');
        } else {
          // Non-consensus task failed: attempt retry; exhaust retries → DLQ.
          if (!task.consensus) {
             const retried = await TaskQueue.retryTask(task.id);
             if (!retried) {
                await TaskQueue.updateTaskStatus(task.id, 'failed', {
                  error: finalError,
                  completedAt: new Date(),
                });
             }
          } else {
             // Consensus evaluation failed (e.g. no majority). Mark the task
             // as failed directly — retrying individual workers independently
             // would require resetting the consensus hash, which adds complexity.
             // Callers should surface this as a task failure and re-submit if needed.
             await TaskQueue.updateTaskStatus(task.id, 'failed', {
               error: finalError,
               completedAt: new Date(),
             });
          }
          logger.error({ taskId: task.id, error: finalError, duration: Date.now() - startTime }, 'Task execution failed');
        }
      } else if (!shouldCompleteTask && executionError && !task.consensus) {
        // Single-task handler threw but consensus count not checked (shouldn't happen, guard).
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
      const worker = await WorkerPool.getWorker(workerId);
      if (worker) {
        await WorkerPool.updateWorkerStatus(workerId, worker.currentTasks > 0 ? 'busy' : 'idle');
      }
    }
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
