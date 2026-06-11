import { TaskQueue } from './task-queue';
import * as redis from './redis';
import { Task } from '../types';

jest.mock('./redis');

const mockedGetRedisClient = redis.getRedisClient as jest.Mock;

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

describe('TaskQueue.getNextTask priority handling', () => {
  it('returns the highest-priority runnable task', async () => {
    // zRange REV returns highest-priority first.
    const store: Record<string, string> = {
      'task:high': JSON.stringify(buildTask({ id: 'high', priority: 'critical' })),
      'task:low': JSON.stringify(buildTask({ id: 'low', priority: 'low' })),
    };
    const client: any = {
      zRange: jest.fn().mockResolvedValue(['high', 'low']),
      get: jest.fn((k: string) => Promise.resolve(store[k] ?? null)),
    };
    mockedGetRedisClient.mockReturnValue(client);

    const next = await TaskQueue.getNextTask('default');
    expect(next?.id).toBe('high');
    // Whole queue scanned, not just a fixed window.
    expect(client.zRange).toHaveBeenCalledWith('queue:default', 0, -1, { REV: true });
  });

  it('does not skip a runnable high-priority task when many higher-sorted tasks are blocked', async () => {
    // 11 blocked tasks (unmet dependency) sort ahead of a runnable task at the end.
    const store: Record<string, string> = {};
    const ids: string[] = [];
    for (let i = 0; i < 11; i++) {
      const id = `blocked${i}`;
      ids.push(id);
      store[`task:${id}`] = JSON.stringify(buildTask({ id, dependencies: ['dep'] }));
    }
    ids.push('runnable');
    store['task:runnable'] = JSON.stringify(buildTask({ id: 'runnable' }));
    // dependency is not completed -> blocked tasks are skipped
    store['task:dep'] = JSON.stringify(buildTask({ id: 'dep', status: 'processing' }));

    const client: any = {
      zRange: jest.fn().mockResolvedValue(ids),
      get: jest.fn((k: string) => Promise.resolve(store[k] ?? null)),
    };
    mockedGetRedisClient.mockReturnValue(client);

    const next = await TaskQueue.getNextTask('default');
    expect(next?.id).toBe('runnable');
  });
});
