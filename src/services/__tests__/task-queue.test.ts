import { TaskQueue } from '../task-queue';
import * as redis from '../redis';
import { Task } from '../../types';

jest.mock('../redis');

const mockedGetRedisClient = redis.getRedisClient as jest.Mock;

/**
 * Build a mock Redis client backed by a simple in-memory string store for
 * get/set/del, with jest.fn() spies for the remaining commands TaskQueue uses.
 * `overrides` lets individual tests stub return values (e.g. zRange, hGetAll).
 */
function makeClient(store: Record<string, string> = {}, overrides: Record<string, any> = {}) {
  const client: any = {
    set: jest.fn((key: string, value: string) => {
      store[key] = value;
      return Promise.resolve('OK');
    }),
    get: jest.fn((key: string) => Promise.resolve(store[key] ?? null)),
    del: jest.fn((key: string) => {
      delete store[key];
      return Promise.resolve(1);
    }),
    zAdd: jest.fn().mockResolvedValue(1),
    zRem: jest.fn().mockResolvedValue(1),
    zRange: jest.fn().mockResolvedValue([]),
    zCard: jest.fn().mockResolvedValue(0),
    hIncrBy: jest.fn().mockResolvedValue(1),
    hGetAll: jest.fn().mockResolvedValue({}),
    lPush: jest.fn().mockResolvedValue(1),
    ...overrides,
  };
  return client;
}

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

describe('TaskQueue.createTask', () => {
  it('creates a task with defaults and persists it', async () => {
    const client = makeClient();
    mockedGetRedisClient.mockReturnValue(client);

    const task = await TaskQueue.createTask('build', 'compile', { repo: 'x' });

    expect(task.name).toBe('build');
    expect(task.handler).toBe('compile');
    expect(task.status).toBe('pending');
    expect(task.priority).toBe('medium');
    expect(task.maxRetries).toBe(3);
    expect(task.queue).toBe('default');
    expect(client.set).toHaveBeenCalledWith(`task:${task.id}`, expect.any(String));
    expect(client.zAdd).toHaveBeenCalled();
    expect(client.hIncrBy).toHaveBeenCalledWith('queue:default:stats', 'pending', 1);
  });

  it('honours provided options', async () => {
    const client = makeClient();
    mockedGetRedisClient.mockReturnValue(client);

    const task = await TaskQueue.createTask('t', 'h', {}, {
      queueName: 'priority',
      priority: 'critical',
      maxRetries: 5,
      timeout: 1000,
    });

    expect(task.queue).toBe('priority');
    expect(task.priority).toBe('critical');
    expect(task.maxRetries).toBe(5);
    expect(task.timeout).toBe(1000);
  });
});

describe('TaskQueue.getTask', () => {
  it('returns the parsed task when present', async () => {
    const task = buildTask();
    const client = makeClient({ 'task:task-1': JSON.stringify(task) });
    mockedGetRedisClient.mockReturnValue(client);

    const result = await TaskQueue.getTask('task-1');
    expect(result?.id).toBe('task-1');
  });

  it('returns null when absent', async () => {
    const client = makeClient();
    mockedGetRedisClient.mockReturnValue(client);

    expect(await TaskQueue.getTask('missing')).toBeNull();
  });
});

describe('TaskQueue.updateTaskStatus', () => {
  it('updates status and sets completedAt for completed', async () => {
    const store = { 'task:task-1': JSON.stringify(buildTask()) };
    const client = makeClient(store);
    mockedGetRedisClient.mockReturnValue(client);

    await TaskQueue.updateTaskStatus('task-1', 'completed');

    const saved = JSON.parse(store['task:task-1']);
    expect(saved.status).toBe('completed');
    expect(saved.completedAt).toBeDefined();
  });

  it('throws when the task does not exist', async () => {
    const client = makeClient();
    mockedGetRedisClient.mockReturnValue(client);

    await expect(TaskQueue.updateTaskStatus('missing', 'completed')).rejects.toThrow(/not found/);
  });
});

describe('TaskQueue.getNextTask', () => {
  it('skips tasks whose dependencies are not completed', async () => {
    const dependent = buildTask({ id: 'dep-task', dependencies: ['base'] });
    const base = buildTask({ id: 'base', status: 'processing' });
    const store = {
      'task:dep-task': JSON.stringify(dependent),
      'task:base': JSON.stringify(base),
    };
    const client = makeClient(store, { zRange: jest.fn().mockResolvedValue(['dep-task']) });
    mockedGetRedisClient.mockReturnValue(client);

    expect(await TaskQueue.getNextTask('default')).toBeNull();
  });

  it('skips tasks scheduled for the future', async () => {
    const future = buildTask({ id: 'later', scheduledFor: new Date(Date.now() + 60_000) });
    const client = makeClient(
      { 'task:later': JSON.stringify(future) },
      { zRange: jest.fn().mockResolvedValue(['later']) }
    );
    mockedGetRedisClient.mockReturnValue(client);

    expect(await TaskQueue.getNextTask('default')).toBeNull();
  });

  it('returns the first eligible task', async () => {
    const ready = buildTask({ id: 'ready' });
    const client = makeClient(
      { 'task:ready': JSON.stringify(ready) },
      { zRange: jest.fn().mockResolvedValue(['ready']) }
    );
    mockedGetRedisClient.mockReturnValue(client);

    const result = await TaskQueue.getNextTask('default');
    expect(result?.id).toBe('ready');
  });
});

describe('TaskQueue.retryTask', () => {
  it('increments retries and re-queues when under the limit', async () => {
    const store = { 'task:task-1': JSON.stringify(buildTask({ retries: 1, maxRetries: 3 })) };
    const client = makeClient(store);
    mockedGetRedisClient.mockReturnValue(client);

    const result = await TaskQueue.retryTask('task-1');

    expect(result).toBe(true);
    const saved = JSON.parse(store['task:task-1']);
    expect(saved.retries).toBe(2);
    expect(saved.status).toBe('retry');
    expect(client.zAdd).toHaveBeenCalled();
  });

  it('moves to the dead letter queue when max retries reached', async () => {
    const store = { 'task:task-1': JSON.stringify(buildTask({ retries: 3, maxRetries: 3 })) };
    const client = makeClient(store);
    mockedGetRedisClient.mockReturnValue(client);

    const result = await TaskQueue.retryTask('task-1');

    expect(result).toBe(false);
    expect(client.lPush).toHaveBeenCalledWith('dlq:tasks', expect.any(String));
  });
});

describe('TaskQueue.cleanupOldTasks', () => {
  it('deletes only completed or failed tasks and returns the count', async () => {
    const store = {
      'task:done': JSON.stringify(buildTask({ id: 'done', status: 'completed' })),
      'task:failed': JSON.stringify(buildTask({ id: 'failed', status: 'failed' })),
      'task:running': JSON.stringify(buildTask({ id: 'running', status: 'processing' })),
    };
    const client = makeClient(store, {
      zRange: jest.fn().mockResolvedValue(['done', 'failed', 'running']),
    });
    mockedGetRedisClient.mockReturnValue(client);

    const deleted = await TaskQueue.cleanupOldTasks(24);

    expect(deleted).toBe(2);
    expect(client.del).toHaveBeenCalledWith('task:done');
    expect(client.del).toHaveBeenCalledWith('task:failed');
    expect(client.del).not.toHaveBeenCalledWith('task:running');
  });
});
