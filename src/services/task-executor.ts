import { Worker as ThreadWorker } from 'worker_threads';
import logger from '../utils/logger';
import { Task, TaskStatus } from '../types';
import { TaskQueue } from './task-queue';
import { WorkerPool } from './worker-pool';

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

    try {
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

      // Execute handler in worker thread to support forceful termination
      const result = await new Promise<any>((resolve, reject) => {
        const handlerStr = handler.toString();
        const workerCode = `
          const { parentPort, workerData } = require('worker_threads');
          
          // Define a dummy logger in global scope to prevent crashes in handlers
          const logger = {
            info: (...args) => console.log(...args),
            error: (...args) => console.error(...args),
            warn: (...args) => console.warn(...args),
            debug: (...args) => console.debug(...args),
          };

          async function run() {
            try {
              // Evaluate the handler function string
              const handlerFn = eval(workerData.handlerStr);
              const result = await handlerFn(workerData.payload);
              parentPort.postMessage({ success: true, result });
            } catch (error) {
              parentPort.postMessage({
                success: false,
                error: error instanceof Error ? error.message : String(error)
              });
            }
          }
          run();
        `;

        const worker = new ThreadWorker(workerCode, {
          eval: true,
          workerData: {
            handlerStr,
            payload: task.payload
          }
        });

        let hasTimedOut = false;

        const timeoutHandle = setTimeout(async () => {
          hasTimedOut = true;
          reject(new Error(`Task execution timeout after ${task.timeout}ms`));
          await worker.terminate();
        }, task.timeout);

        worker.on('message', (msg) => {
          if (hasTimedOut) return;
          clearTimeout(timeoutHandle);
          if (msg.success) {
            resolve(msg.result);
          } else {
            reject(new Error(msg.error));
          }
        });

        worker.on('error', (err) => {
          if (hasTimedOut) return;
          clearTimeout(timeoutHandle);
          reject(err);
        });

        worker.on('exit', (code) => {
          if (hasTimedOut) return;
          clearTimeout(timeoutHandle);
          if (code !== 0) {
            reject(new Error(`Worker thread exited with code ${code}`));
          }
        });
      });

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

      logger.info({ taskId: task.id, duration }, 'Task completed successfully');
    } catch (error: any) {
      const duration = Date.now() - startTime;
      const errorMessage = error?.message || String(error);

      logger.error({ taskId: task.id, error: errorMessage, duration }, 'Task execution failed');

      // Attempt retry
      const retried = await TaskQueue.retryTask(task.id);

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
}
