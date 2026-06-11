import logger, { withTrace } from '../utils/logger';
import { Task, TaskStatus } from '../types';
import { TaskQueue } from './task-queue';
import { WorkerPool } from './worker-pool';

export interface TaskContext {
  taskId: string;
  traceId?: string;
  /** spanId of the parent task; pass this as `parentSpanId` when creating child tasks. */
  parentSpanId?: string;
}

export interface TaskHandler {
  (payload: Record<string, any>, context: TaskContext): Promise<any>;
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

      // Execute with timeout
      const context: TaskContext = {
        taskId: task.id,
        traceId: task.traceId,
        parentSpanId: task.parentSpanId,
      };
      const result = await Promise.race([
        handler(task.payload, context),
        new Promise<never>((_, reject) => {
          timeoutHandle = setTimeout(() => {
            reject(new Error(`Task execution timeout after ${task.timeout}ms`));
          }, task.timeout);
        }),
      ]);

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

      const tlog = task.traceId
        ? withTrace({ trace_id: task.traceId, span_id: task.id, parent_span_id: task.parentSpanId })
        : logger;
      tlog.info({ taskId: task.id, duration }, 'Task completed successfully');
    } catch (error: any) {
      const duration = Date.now() - startTime;
      const errorMessage = error?.message || String(error);

      const tlog = task.traceId
        ? withTrace({ trace_id: task.traceId, span_id: task.id, parent_span_id: task.parentSpanId })
        : logger;
      tlog.error({ taskId: task.id, error: errorMessage, duration }, 'Task execution failed');

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
