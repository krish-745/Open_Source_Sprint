import { TaskExecutor } from './task-executor';
import { WorkerPool } from './worker-pool';
import { TaskQueue } from './task-queue';
import { Task } from '../types';

jest.mock('./worker-pool');
jest.mock('./task-queue');

const mockedWorkerPool = WorkerPool as jest.Mocked<typeof WorkerPool>;
const mockedTaskQueue = TaskQueue as jest.Mocked<typeof TaskQueue>;

function buildTask(overrides: Partial<Task> = {}): Task {
  return {
    id: 'task-1',
    name: 'demo',
    description: 'Task: demo',
    priority: 'medium',
    status: 'queued',
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
    ...overrides,
  };
}

describe('TaskExecutor cancellation', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    TaskExecutor.clearHandlers();
    TaskExecutor.clearCancellation('task-1');
    mockedTaskQueue.updateTaskStatus.mockResolvedValue(undefined as any);
    mockedWorkerPool.updateWorkerStatus.mockResolvedValue(undefined as any);
    mockedWorkerPool.completeTask.mockResolvedValue(undefined as any);
    mockedWorkerPool.getWorker.mockResolvedValue({ currentTasks: 0 } as any);
  });

  it('tracks a cancellation signal', () => {
    expect(TaskExecutor.isCancelled('task-1')).toBe(false);
    TaskExecutor.cancel('task-1');
    expect(TaskExecutor.isCancelled('task-1')).toBe(true);
  });

  it('does not run the handler for a task cancelled before execution', async () => {
    const handler = jest.fn().mockResolvedValue('ok');
    TaskExecutor.registerHandler('noop', handler);
    TaskExecutor.cancel('task-1');

    await TaskExecutor.execute('w1', buildTask());

    expect(handler).not.toHaveBeenCalled();
    expect(mockedTaskQueue.updateTaskStatus).toHaveBeenCalledWith('task-1', 'cancelled');
  });

  it('passes a cancellation-aware context to the handler', async () => {
    let sawContext = false;
    TaskExecutor.registerHandler('noop', async (_payload, context) => {
      sawContext = typeof context?.isCancelled === 'function';
      return 'ok';
    });

    await TaskExecutor.execute('w1', buildTask());

    expect(sawContext).toBe(true);
  });
});
