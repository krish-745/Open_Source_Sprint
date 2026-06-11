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

describe('TaskQueue.updateTaskStatus queue stats', () => {
  afterEach(() => jest.clearAllMocks());

  it('decrements the previous bucket and increments the new one on transition', async () => {
    const task = buildTask({ status: 'pending', queue: 'default' });
    const hIncrBy = jest.fn().mockResolvedValue(1);
    const mockClient = {
      get: jest.fn().mockResolvedValue(JSON.stringify(task)),
      set: jest.fn().mockResolvedValue('OK'),
      hIncrBy,
    };
    mockedGetRedisClient.mockReturnValue(mockClient as any);

    await TaskQueue.updateTaskStatus('task-1', 'processing');

    expect(hIncrBy).toHaveBeenCalledWith('queue:default:stats', 'pending', -1);
    expect(hIncrBy).toHaveBeenCalledWith('queue:default:stats', 'processing', 1);
  });

  it('only decrements pending when moving to an untracked status (queued)', async () => {
    const task = buildTask({ status: 'pending', queue: 'default' });
    const hIncrBy = jest.fn().mockResolvedValue(1);
    const mockClient = {
      get: jest.fn().mockResolvedValue(JSON.stringify(task)),
      set: jest.fn().mockResolvedValue('OK'),
      hIncrBy,
    };
    mockedGetRedisClient.mockReturnValue(mockClient as any);

    await TaskQueue.updateTaskStatus('task-1', 'queued');

    expect(hIncrBy).toHaveBeenCalledWith('queue:default:stats', 'pending', -1);
    expect(hIncrBy).toHaveBeenCalledTimes(1); // 'queued' is not a tracked bucket
  });

  it('does not touch stats when the status is unchanged', async () => {
    const task = buildTask({ status: 'processing', queue: 'default' });
    const hIncrBy = jest.fn().mockResolvedValue(1);
    const mockClient = {
      get: jest.fn().mockResolvedValue(JSON.stringify(task)),
      set: jest.fn().mockResolvedValue('OK'),
      hIncrBy,
    };
    mockedGetRedisClient.mockReturnValue(mockClient as any);

    await TaskQueue.updateTaskStatus('task-1', 'processing');

    expect(hIncrBy).not.toHaveBeenCalled();
  });
});
