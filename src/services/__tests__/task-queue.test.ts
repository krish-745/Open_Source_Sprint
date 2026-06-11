import { TaskQueue } from '../task-queue';
import { getRedisClient } from '../redis';
import { Task } from '../../types';

// Mock the Redis client using an in-memory store
let dlqList: string[] = [];
const store: Record<string, string> = {};

const mockRedisClient = {
  get: jest.fn().mockImplementation(async (key: string) => store[key] || null),
  set: jest.fn().mockImplementation(async (key: string, value: string) => {
    store[key] = value;
    return 'OK';
  }),
  zAdd: jest.fn().mockResolvedValue(1),
  hIncrBy: jest.fn().mockResolvedValue(1),
  lRange: jest.fn().mockImplementation(async (key: string, start: number, stop: number) => {
    return dlqList;
  }),
  lLen: jest.fn().mockImplementation(async (key: string) => {
    return dlqList.length;
  }),
  lRem: jest.fn().mockImplementation(async (key: string, count: number, value: string) => {
    const idx = dlqList.indexOf(value);
    if (idx !== -1) {
      dlqList.splice(idx, 1);
      return 1;
    }
    return 0;
  }),
};

jest.mock('../redis', () => ({
  getRedisClient: () => mockRedisClient,
}));

describe('TaskQueue - DLQ Inspection and Management', () => {
  beforeEach(() => {
    dlqList = [];
    for (const key in store) delete store[key];
    jest.clearAllMocks();
  });

  const createTaskMock = (id: string, overrides: Partial<Task>): Task => {
    return {
      id,
      name: `Task ${id}`,
      description: `Task description`,
      priority: 'medium',
      status: 'failed',
      handler: 'testHandler',
      payload: {},
      retries: 3,
      maxRetries: 3,
      timeout: 30000,
      createdAt: new Date(),
      completedAt: new Date(),
      queue: 'default',
      dependencies: [],
      tags: [],
      metadata: {},
      error: 'TimeoutError: Execution timed out',
      ...overrides,
    };
  };

  it('should list and filter DLQ tasks correctly', async () => {
    const task1 = createTaskMock('1', { error: 'TimeoutError: connection lost', completedAt: new Date('2026-06-01T00:00:00Z') });
    const task2 = createTaskMock('2', { error: 'TypeError: undefined variable', completedAt: new Date('2026-06-05T00:00:00Z') });

    dlqList.push(JSON.stringify(task1), JSON.stringify(task2));

    // Test get all
    const all = await TaskQueue.getDLQTasks();
    expect(all.length).toBe(2);

    // Filter by error type
    const typeErrorMatches = await TaskQueue.getDLQTasks({ errorType: 'TypeError' });
    expect(typeErrorMatches.length).toBe(1);
    expect(typeErrorMatches[0].id).toBe('2');

    // Filter by date range
    const dateMatches = await TaskQueue.getDLQTasks({
      startDate: new Date('2026-06-03T00:00:00Z'),
    });
    expect(dateMatches.length).toBe(1);
    expect(dateMatches[0].id).toBe('2');
  });

  it('should generate DLQ stats correctly', async () => {
    const task1 = createTaskMock('1', { queue: 'default', error: 'TimeoutError: timeout' });
    const task2 = createTaskMock('2', { queue: 'high-priority', error: 'TypeError: type' });
    const task3 = createTaskMock('3', { queue: 'default', error: 'TimeoutError: another' });

    dlqList.push(JSON.stringify(task1), JSON.stringify(task2), JSON.stringify(task3));

    const stats = await TaskQueue.getDLQStats();
    expect(stats.size).toBe(3);
    expect(stats.queues).toEqual({
      'default': 2,
      'high-priority': 1,
    });
    expect(stats.errors).toEqual({
      'TimeoutError:': 2,
      'TypeError:': 1,
    });
  });

  it('should successfully retry task from DLQ', async () => {
    const task = createTaskMock('test-retry-id', { queue: 'default' });
    const rawTask = JSON.stringify(task);
    dlqList.push(rawTask);

    const success = await TaskQueue.retryDLQTask('test-retry-id');
    expect(success).toBe(true);
    expect(dlqList.length).toBe(0);

    const restoredTaskData = store['task:test-retry-id'];
    expect(restoredTaskData).toBeDefined();
    const restoredTask: Task = JSON.parse(restoredTaskData);
    expect(restoredTask.status).toBe('pending');
    expect(restoredTask.retries).toBe(0);
    expect(restoredTask.error).toBeUndefined();
  });

  it('should successfully delete task from DLQ', async () => {
    const task = createTaskMock('test-delete-id', {});
    const rawTask = JSON.stringify(task);
    dlqList.push(rawTask);

    const success = await TaskQueue.deleteDLQTask('test-delete-id');
    expect(success).toBe(true);
    expect(dlqList.length).toBe(0);
  });
});
