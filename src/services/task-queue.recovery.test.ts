import { TaskQueue } from './task-queue';
import * as redis from './redis';
import { Task } from '../types';

jest.mock('./redis');

const mockedGetRedisClient = redis.getRedisClient as jest.Mock;

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

afterEach(() => jest.clearAllMocks());

describe('TaskQueue.recoverStaleTasks', () => {
  it('re-queues processing tasks that have been stuck longer than the timeout', async () => {
    const stale = buildTask({
      id: 'stale',
      status: 'processing',
      workerId: 'dead-worker',
      startedAt: new Date(Date.now() - 10 * 60 * 1000), // 10 min ago
    });
    const fresh = buildTask({
      id: 'fresh',
      status: 'processing',
      startedAt: new Date(Date.now() - 1000), // 1s ago
    });
    const done = buildTask({ id: 'done', status: 'completed' });

    const store: Record<string, string> = {
      'task:stale': JSON.stringify(stale),
      'task:fresh': JSON.stringify(fresh),
      'task:done': JSON.stringify(done),
    };
    const client: any = {
      zRange: jest.fn().mockResolvedValue(['stale', 'fresh', 'done']),
      get: jest.fn((key: string) => Promise.resolve(store[key] ?? null)),
      set: jest.fn((key: string, value: string) => {
        store[key] = value;
        return Promise.resolve('OK');
      }),
      zAdd: jest.fn().mockResolvedValue(1),
    };
    mockedGetRedisClient.mockReturnValue(client);

    const recovered = await TaskQueue.recoverStaleTasks(5 * 60 * 1000);

    expect(recovered).toBe(1);
    const saved = JSON.parse(store['task:stale']);
    expect(saved.status).toBe('queued');
    expect(saved.workerId).toBeUndefined();
    expect(client.zAdd).toHaveBeenCalledWith('queue:default', expect.objectContaining({ value: 'stale' }));
    // fresh and done are untouched
    expect(JSON.parse(store['task:fresh']).status).toBe('processing');
    expect(JSON.parse(store['task:done']).status).toBe('completed');
  });

  it('returns 0 when there are no stale tasks', async () => {
    const fresh = buildTask({ id: 'fresh', status: 'processing', startedAt: new Date() });
    const client: any = {
      zRange: jest.fn().mockResolvedValue(['fresh']),
      get: jest.fn().mockResolvedValue(JSON.stringify(fresh)),
      set: jest.fn(),
      zAdd: jest.fn(),
    };
    mockedGetRedisClient.mockReturnValue(client);

    expect(await TaskQueue.recoverStaleTasks(5 * 60 * 1000)).toBe(0);
    expect(client.zAdd).not.toHaveBeenCalled();
  });
});
