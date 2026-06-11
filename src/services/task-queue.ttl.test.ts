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

describe('TaskQueue TTL', () => {
  it('stores ttl and a derived expiresAt on createTask', async () => {
    const client: any = {
      set: jest.fn().mockResolvedValue('OK'),
      zAdd: jest.fn().mockResolvedValue(1),
      hIncrBy: jest.fn().mockResolvedValue(1),
    };
    mockedGetRedisClient.mockReturnValue(client);

    const task = await TaskQueue.createTask('t', 'h', {}, { ttl: 60 });
    expect(task.ttl).toBe(60);
    expect(task.expiresAt).toBeDefined();
    expect(new Date(task.expiresAt!).getTime()).toBeGreaterThan(Date.now());
  });

  it('applies a priority-based default ttl when none is given', async () => {
    const client: any = {
      set: jest.fn().mockResolvedValue('OK'),
      zAdd: jest.fn().mockResolvedValue(1),
      hIncrBy: jest.fn().mockResolvedValue(1),
    };
    mockedGetRedisClient.mockReturnValue(client);

    const critical = await TaskQueue.createTask('t', 'h', {}, { priority: 'critical' });
    const low = await TaskQueue.createTask('t', 'h', {}, { priority: 'low' });
    expect(critical.ttl).toBeGreaterThan(low.ttl!);
  });

  it('isExpired is true only for un-started tasks past expiry', () => {
    const past = new Date(Date.now() - 1000);
    const future = new Date(Date.now() + 60_000);
    expect(TaskQueue.isExpired(buildTask({ status: 'pending', expiresAt: past }))).toBe(true);
    expect(TaskQueue.isExpired(buildTask({ status: 'processing', expiresAt: past }))).toBe(false);
    expect(TaskQueue.isExpired(buildTask({ status: 'pending', expiresAt: future }))).toBe(false);
    expect(TaskQueue.isExpired(buildTask({ status: 'pending' }))).toBe(false);
  });

  it('expireStaleTasks cancels expired pending tasks and removes them from the queue', async () => {
    const store: Record<string, string> = {
      'task:expired': JSON.stringify(
        buildTask({ id: 'expired', status: 'pending', expiresAt: new Date(Date.now() - 1000) })
      ),
      'task:fresh': JSON.stringify(
        buildTask({ id: 'fresh', status: 'pending', expiresAt: new Date(Date.now() + 60_000) })
      ),
    };
    const client: any = {
      zRange: jest.fn().mockResolvedValue(['expired', 'fresh']),
      get: jest.fn((k: string) => Promise.resolve(store[k] ?? null)),
      set: jest.fn((k: string, v: string) => {
        store[k] = v;
        return Promise.resolve('OK');
      }),
      zRem: jest.fn().mockResolvedValue(1),
    };
    mockedGetRedisClient.mockReturnValue(client);

    const expired = await TaskQueue.expireStaleTasks();
    expect(expired).toBe(1);
    expect(JSON.parse(store['task:expired']).status).toBe('cancelled');
    expect(client.zRem).toHaveBeenCalledWith('queue:default', 'expired');
    expect(JSON.parse(store['task:fresh']).status).toBe('pending');
  });
});
