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

function makeClient(store: Record<string, string> = {}) {
  return {
    get: jest.fn((k: string) => Promise.resolve(store[k] ?? null)),
    set: jest.fn((k: string, v: string) => {
      store[k] = v;
      return Promise.resolve('OK');
    }),
    zAdd: jest.fn().mockResolvedValue(1),
    zCard: jest.fn().mockResolvedValue(0),
    zRange: jest.fn().mockResolvedValue([]),
    hIncrBy: jest.fn().mockResolvedValue(1),
    __store: store,
  } as any;
}

afterEach(() => jest.clearAllMocks());

describe('TaskQueue distributed tracing', () => {
  it('generates a trace id when none is provided', async () => {
    mockedGetRedisClient.mockReturnValue(makeClient());
    const task = await TaskQueue.createTask('t', 'h', {});
    expect(typeof task.traceId).toBe('string');
    expect(task.traceId).toHaveLength(36); // uuid v4
  });

  it('honours an explicit trace id', async () => {
    mockedGetRedisClient.mockReturnValue(makeClient());
    const task = await TaskQueue.createTask('t', 'h', {}, { traceId: 'trace-abc' });
    expect(task.traceId).toBe('trace-abc');
  });

  it('inherits the trace id from the first dependency', async () => {
    const store = {
      'task:parent': JSON.stringify(buildTask({ id: 'parent', traceId: 'trace-parent' })),
    };
    mockedGetRedisClient.mockReturnValue(makeClient(store));

    const child = await TaskQueue.createTask('child', 'h', {}, { dependencies: ['parent'] });
    expect(child.traceId).toBe('trace-parent');
  });

  it('queries tasks by trace id', async () => {
    const store = {
      'task:a': JSON.stringify(buildTask({ id: 'a', traceId: 'trace-1' })),
      'task:b': JSON.stringify(buildTask({ id: 'b', traceId: 'trace-1' })),
      'task:c': JSON.stringify(buildTask({ id: 'c', traceId: 'trace-2' })),
    };
    const client = makeClient(store);
    client.zRange = jest.fn().mockResolvedValue(['a', 'b', 'c']);
    mockedGetRedisClient.mockReturnValue(client);

    const traced = await TaskQueue.getTasksByTrace('trace-1');
    expect(traced.map((t) => t.id).sort()).toEqual(['a', 'b']);
  });
});
