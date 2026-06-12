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

function makeClient(store: Record<string, string>) {
  return {
    get: jest.fn((k: string) => Promise.resolve(store[k] ?? null)),
    set: jest.fn((k: string, v: string) => {
      store[k] = v;
      return Promise.resolve('OK');
    }),
    zAdd: jest.fn().mockResolvedValue(1),
  } as any;
}

afterEach(() => jest.clearAllMocks());

describe('TaskQueue.updateTaskFields', () => {
  it('updates allowed fields on a pending task and records an audit entry', async () => {
    const store = { 'task:task-1': JSON.stringify(buildTask()) };
    const client = makeClient(store);
    mockedGetRedisClient.mockReturnValue(client);

    const updated = await TaskQueue.updateTaskFields('task-1', { priority: 'high', timeout: 60000 });

    expect(updated.priority).toBe('high');
    expect(updated.timeout).toBe(60000);
    expect(updated.metadata.auditLog).toHaveLength(1);
    // priority change re-scores the queue entry
    expect(client.zAdd).toHaveBeenCalledWith('queue:default', expect.objectContaining({ value: 'task-1' }));
  });

  it('refuses to modify a processing task', async () => {
    const store = { 'task:task-1': JSON.stringify(buildTask({ status: 'processing' })) };
    mockedGetRedisClient.mockReturnValue(makeClient(store));

    await expect(TaskQueue.updateTaskFields('task-1', { priority: 'high' })).rejects.toThrow(/cannot be modified/);
  });

  it('validates field values', async () => {
    const store = { 'task:task-1': JSON.stringify(buildTask()) };
    mockedGetRedisClient.mockReturnValue(makeClient(store));

    await expect(TaskQueue.updateTaskFields('task-1', { timeout: 0 })).rejects.toThrow(/timeout must be positive/);
  });

  it('throws when the task does not exist', async () => {
    mockedGetRedisClient.mockReturnValue(makeClient({}));
    await expect(TaskQueue.updateTaskFields('missing', { priority: 'low' })).rejects.toThrow(/not found/);
  });
});
