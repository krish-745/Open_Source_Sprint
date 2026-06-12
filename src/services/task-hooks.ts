import logger from '../utils/logger';
import { Task, Worker } from '../types';

export type TaskLifecycleEvent =
  | 'task.created'
  | 'task.started'
  | 'task.completed'
  | 'task.failed'
  | 'task.retried';

export type WorkerLifecycleEvent =
  | 'worker.registered'
  | 'worker.offline'
  | 'worker.unregistered';

export type LifecycleEvent = TaskLifecycleEvent | WorkerLifecycleEvent;

export type HookHandler = (context: Record<string, any>) => void | Promise<void>;

/**
 * Lifecycle hook registry for external monitoring/observability.
 *
 * Handlers can be registered for task and worker lifecycle events and receive
 * the full context. A throwing or rejecting hook is logged but never breaks
 * task execution — hooks are best-effort side channels.
 */
export class TaskHooks {
  private static handlers: Map<LifecycleEvent, HookHandler[]> = new Map();

  static on(event: LifecycleEvent, handler: HookHandler): void {
    const list = this.handlers.get(event) ?? [];
    list.push(handler);
    this.handlers.set(event, list);
  }

  static off(event: LifecycleEvent): void {
    this.handlers.delete(event);
  }

  static clear(): void {
    this.handlers.clear();
  }

  /** Fire all handlers for an event; isolate and log handler errors. */
  static async emit(event: LifecycleEvent, context: Record<string, any>): Promise<void> {
    const list = this.handlers.get(event);
    if (!list || list.length === 0) {
      return;
    }
    await Promise.all(
      list.map(async (handler) => {
        try {
          await handler({ event, ...context });
        } catch (error) {
          logger.error({ event, error }, 'Lifecycle hook failed (ignored)');
        }
      })
    );
  }

  static async emitTask(event: TaskLifecycleEvent, task: Task): Promise<void> {
    await this.emit(event, { task });
  }

  static async emitWorker(event: WorkerLifecycleEvent, worker: Worker): Promise<void> {
    await this.emit(event, { worker });
  }
}
