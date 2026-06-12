import { TaskQueue } from './task-queue';
import { TaskExecutor } from './task-executor';
import { WorkerPool } from './worker-pool';
import * as redis from './redis';
import { Task } from '../types';

jest.mock('./redis');
jest.mock('./worker-pool');

const mockedGetRedisClient = redis.getRedisClient as jest.Mock;
const mockedWorkerPool = WorkerPool as jest.Mocked<typeof WorkerPool>;

function buildTask(overrides: Partial<Task> = {}): Task {
  return {
    id: 'task',
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

afterEach(() => jest.clearAllMocks());

describe('TaskQueue.getNextBatch', () => {
  it('returns up to batchSize runnable tasks in priority order', async () => {
    const store: Record<string, string> = {};
    const ids = ['t1', 't2', 't3', 't4'];
    ids.forEach((id) => (store[`task:${id}`] = JSON.stringify(buildTask({ id }))));
    const client: any = {
      zRange: jest.fn().mockResolvedValue(ids),
      get: jest.fn((k: string) => Promise.resolve(store[k] ?? null)),
      zRem: jest.fn().mockResolvedValue(1),
    };
    mockedGetRedisClient.mockReturnValue(client);

    const batch = await TaskQueue.getNextBatch('default', 2);
    expect(batch.map((t) => t.id)).toEqual(['t1', 't2']);
  });

  it('skips tasks with unmet dependencies', async () => {
    const store: Record<string, string> = {
      'task:blocked': JSON.stringify(buildTask({ id: 'blocked', dependencies: ['dep'] })),
      'task:ready': JSON.stringify(buildTask({ id: 'ready' })),
      'task:dep': JSON.stringify(buildTask({ id: 'dep', status: 'processing' })),
    };
    const client: any = {
      zRange: jest.fn().mockResolvedValue(['blocked', 'ready']),
      get: jest.fn((k: string) => Promise.resolve(store[k] ?? null)),
      zRem: jest.fn().mockResolvedValue(1),
    };
    mockedGetRedisClient.mockReturnValue(client);

    const batch = await TaskQueue.getNextBatch('default', 10);
    expect(batch.map((t) => t.id)).toEqual(['ready']);
  });
});

describe('TaskExecutor.executeBatch', () => {
  beforeEach(() => {
    TaskExecutor.clearHandlers();
    mockedWorkerPool.updateWorkerStatus.mockResolvedValue(undefined as any);
    mockedWorkerPool.completeTask.mockResolvedValue(undefined as any);
    mockedWorkerPool.getWorker.mockResolvedValue({ currentTasks: 0 } as any);
  });

  it('processes all tasks and survives partial failure', async () => {
    const executeSpy = jest
      .spyOn(TaskExecutor, 'execute')
      .mockImplementation(async (_w, task) => {
        if (task.id === 'bad') throw new Error('boom');
      });

    const summary = await TaskExecutor.executeBatch('w1', [
      buildTask({ id: 'ok1' }),
      buildTask({ id: 'bad' }),
      buildTask({ id: 'ok2' }),
    ]);

    expect(summary.total).toBe(3);
    expect(summary.succeeded).toBe(2);
    expect(summary.failed).toBe(1);
    expect(executeSpy).toHaveBeenCalledTimes(3);
    executeSpy.mockRestore();
  });
});
