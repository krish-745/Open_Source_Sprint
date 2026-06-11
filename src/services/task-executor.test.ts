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
    ...overrides,
  };
}

describe('TaskExecutor.execute worker status', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    TaskExecutor.clearHandlers();
    mockedTaskQueue.updateTaskStatus.mockResolvedValue(undefined as any);
    mockedWorkerPool.updateWorkerStatus.mockResolvedValue(undefined as any);
    mockedWorkerPool.completeTask.mockResolvedValue(undefined as any);
    TaskExecutor.registerHandler('noop', async () => 'ok');
  });

  it('keeps the worker busy when other tasks are still running after completion', async () => {
    mockedWorkerPool.getWorker.mockResolvedValue({ currentTasks: 2 } as any);

    await TaskExecutor.execute('w1', buildTask());

    expect(mockedWorkerPool.updateWorkerStatus).toHaveBeenLastCalledWith('w1', 'busy');
  });

  it('sets the worker idle when no tasks remain', async () => {
    mockedWorkerPool.getWorker.mockResolvedValue({ currentTasks: 0 } as any);

    await TaskExecutor.execute('w1', buildTask());

    expect(mockedWorkerPool.updateWorkerStatus).toHaveBeenLastCalledWith('w1', 'idle');
  });
});
