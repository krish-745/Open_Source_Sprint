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

describe('TaskQueue.cancelTask', () => {
  it('cancels a non-terminal task and removes it from the queue', async () => {
    const store: Record<string, string> = { 'task:task-1': JSON.stringify(buildTask()) };
    const client: any = {
      get: jest.fn((k: string) => Promise.resolve(store[k] ?? null)),
      set: jest.fn((k: string, v: string) => {
        store[k] = v;
        return Promise.resolve('OK');
      }),
      zRem: jest.fn().mockResolvedValue(1),
    };
    mockedGetRedisClient.mockReturnValue(client);

    const result = await TaskQueue.cancelTask('task-1');

    expect(result).toBe(true);
    expect(JSON.parse(store['task:task-1']).status).toBe('cancelled');
    expect(client.zRem).toHaveBeenCalledWith('queue:default', 'task-1');
  });

  it('does not cancel a completed task', async () => {
    const client: any = {
      get: jest.fn().mockResolvedValue(JSON.stringify(buildTask({ status: 'completed' }))),
      set: jest.fn(),
      zRem: jest.fn(),
    };
    mockedGetRedisClient.mockReturnValue(client);

    expect(await TaskQueue.cancelTask('task-1')).toBe(false);
    expect(client.zRem).not.toHaveBeenCalled();
  });

  it('throws when the task does not exist', async () => {
    const client: any = { get: jest.fn().mockResolvedValue(null), set: jest.fn(), zRem: jest.fn() };
    mockedGetRedisClient.mockReturnValue(client);

    await expect(TaskQueue.cancelTask('missing')).rejects.toThrow(/not found/);
  });
});
