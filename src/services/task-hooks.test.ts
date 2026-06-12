import { TaskHooks } from './task-hooks';
import { Task } from '../types';

function buildTask(): Task {
  return {
    id: 'task-1',
    name: 'demo',
    description: 'Task: demo',
    priority: 'medium',
    status: 'pending',
    handler: 'noop',
    payload: {},
    retries: 0,
    maxRetries: 3,
    timeout: 30000,
    createdAt: new Date(),
    queue: 'default',
    dependencies: [],
    tags: [],
    metadata: {},
  };
}

afterEach(() => TaskHooks.clear());

describe('TaskHooks', () => {
  it('fires registered handlers with the event and context', async () => {
    const received: any[] = [];
    TaskHooks.on('task.completed', (ctx) => {
      received.push(ctx);
    });

    await TaskHooks.emitTask('task.completed', buildTask());

    expect(received).toHaveLength(1);
    expect(received[0].event).toBe('task.completed');
    expect(received[0].task.id).toBe('task-1');
  });

  it('supports multiple handlers for one event', async () => {
    let count = 0;
    TaskHooks.on('task.started', () => {
      count++;
    });
    TaskHooks.on('task.started', () => {
      count++;
    });

    await TaskHooks.emitTask('task.started', buildTask());
    expect(count).toBe(2);
  });

  it('does not let a throwing hook break execution and still runs other hooks', async () => {
    let secondRan = false;
    TaskHooks.on('task.failed', () => {
      throw new Error('hook boom');
    });
    TaskHooks.on('task.failed', () => {
      secondRan = true;
    });

    await expect(TaskHooks.emitTask('task.failed', buildTask())).resolves.toBeUndefined();
    expect(secondRan).toBe(true);
  });

  it('is a no-op when no handlers are registered', async () => {
    await expect(TaskHooks.emitTask('task.created', buildTask())).resolves.toBeUndefined();
  });
});
