import logger from '../utils/logger';
import { Task, TaskStatus } from '../types';
import { TaskQueue } from './task-queue';
import { WorkerPool } from './worker-pool';

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
