import { WatchError } from 'redis';
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

/**
 * Build a mock client whose MULTI.exec() behaviour is supplied per test so we
 * can simulate WATCH conflicts (WatchError) and successful commits.
 */
function makeClient(taskJson: string | null, execImpl: jest.Mock) {
  const setSpy = jest.fn().mockReturnThis();
  const multi = { 
    set: setSpy, 
    zAdd: jest.fn().mockReturnThis(), 
    zRem: jest.fn().mockReturnThis(), 
    exec: execImpl 
  };
  const client: any = {
    watch: jest.fn().mockResolvedValue('OK'),
    unwatch: jest.fn().mockResolvedValue('OK'),
    executeIsolated: jest.fn().mockImplementation(async (callback) => {
      return callback(client);
    }),
    get: jest.fn().mockResolvedValue(taskJson),
    multi: jest.fn().mockReturnValue(multi),
    hIncrBy: jest.fn().mockResolvedValue(1),
    __setSpy: setSpy,
  };
  return client;
}

afterEach(() => jest.clearAllMocks());

describe('TaskQueue.updateTaskStatus atomicity', () => {
  it('commits the update inside a WATCH/MULTI transaction', async () => {
    const exec = jest.fn().mockResolvedValue([]);
    const client = makeClient(JSON.stringify(buildTask()), exec);
    mockedGetRedisClient.mockReturnValue(client);

    await TaskQueue.updateTaskStatus('task-1', 'completed');

    expect(client.watch).toHaveBeenCalledWith('task:task-1');
    expect(client.multi).toHaveBeenCalledTimes(1);
    expect(exec).toHaveBeenCalledTimes(1);
    const savedJson = client.__setSpy.mock.calls[0][1];
    expect(JSON.parse(savedJson).status).toBe('completed');
  });

  it('retries when a concurrent modification aborts the transaction', async () => {
    const exec = jest
      .fn()
      .mockRejectedValueOnce(new WatchError())
      .mockResolvedValueOnce([]);
    const client = makeClient(JSON.stringify(buildTask()), exec);
    mockedGetRedisClient.mockReturnValue(client);

    await TaskQueue.updateTaskStatus('task-1', 'processing');

    expect(client.watch).toHaveBeenCalledTimes(2);
    expect(exec).toHaveBeenCalledTimes(2);
  });

  it('throws after exhausting retries on persistent conflicts', async () => {
    const exec = jest.fn().mockRejectedValue(new WatchError());
    const client = makeClient(JSON.stringify(buildTask()), exec);
    mockedGetRedisClient.mockReturnValue(client);

    await expect(TaskQueue.updateTaskStatus('task-1', 'completed')).rejects.toThrow(/after 5 attempts/);
    expect(exec).toHaveBeenCalledTimes(5);
  });

  it('unwatches and throws when the task does not exist', async () => {
    const exec = jest.fn();
    const client = makeClient(null, exec);
    mockedGetRedisClient.mockReturnValue(client);

    await expect(TaskQueue.updateTaskStatus('missing', 'completed')).rejects.toThrow(/not found/);
    expect(client.unwatch).toHaveBeenCalled();
    expect(exec).not.toHaveBeenCalled();
  });
});
