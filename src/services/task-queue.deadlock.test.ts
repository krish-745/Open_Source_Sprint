import { TaskQueue, DependencyCycleError } from './task-queue';
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

function makeClient(store: Record<string, string>) {
  return {
    get: jest.fn((key: string) => Promise.resolve(store[key] ?? null)),
    set: jest.fn((key: string, value: string) => {
      store[key] = value;
      return Promise.resolve('OK');
    }),
    zAdd: jest.fn().mockResolvedValue(1),
    zCard: jest.fn().mockResolvedValue(0),
    hIncrBy: jest.fn().mockResolvedValue(1),
  } as any;
}

afterEach(() => jest.clearAllMocks());

describe('TaskQueue.createTask dependency cycle detection', () => {
  it('rejects a task whose dependencies form a cycle (A -> B -> A)', async () => {
    const store = {
      'task:A': JSON.stringify(buildTask({ id: 'A', dependencies: ['B'] })),
      'task:B': JSON.stringify(buildTask({ id: 'B', dependencies: ['A'] })),
    };
    mockedGetRedisClient.mockReturnValue(makeClient(store));

    await expect(
      TaskQueue.createTask('t', 'h', {}, { dependencies: ['A'] })
    ).rejects.toBeInstanceOf(DependencyCycleError);
  });

  it('exposes the offending cycle on the error', async () => {
    const store = {
      'task:A': JSON.stringify(buildTask({ id: 'A', dependencies: ['B'] })),
      'task:B': JSON.stringify(buildTask({ id: 'B', dependencies: ['A'] })),
    };
    mockedGetRedisClient.mockReturnValue(makeClient(store));

    try {
      await TaskQueue.createTask('t', 'h', {}, { dependencies: ['A'] });
      throw new Error('expected createTask to throw');
    } catch (err) {
      expect(err).toBeInstanceOf(DependencyCycleError);
      expect((err as DependencyCycleError).cycle).toEqual(['A', 'B', 'A']);
    }
  });

  it('allows an acyclic transitive dependency chain (A -> B -> C)', async () => {
    const store = {
      'task:A': JSON.stringify(buildTask({ id: 'A', dependencies: ['B'] })),
      'task:B': JSON.stringify(buildTask({ id: 'B', dependencies: ['C'] })),
      'task:C': JSON.stringify(buildTask({ id: 'C', dependencies: [] })),
    };
    mockedGetRedisClient.mockReturnValue(makeClient(store));

    const task = await TaskQueue.createTask('t', 'h', {}, { dependencies: ['A'] });
    expect(task.dependencies).toEqual(['A']);
    expect(task.status).toBe('pending');
  });

  it('allows a task with no dependencies', async () => {
    mockedGetRedisClient.mockReturnValue(makeClient({}));

    const task = await TaskQueue.createTask('t', 'h', {});
    expect(task.id).toBeDefined();
  });
});
