import { TaskQueue } from '../task-queue';
import { getRedisClient } from '../redis';
import { Task } from '../../types';

// Mock the Redis client using an in-memory store
const store: Record<string, string> = {};
const statsStore: Record<string, number> = {
  pending: 0,
  processing: 0,
  completed: 0,
  failed: 0,
};

const mockRedisClient = {
  get: jest.fn().mockImplementation(async (key: string) => store[key] || null),
  set: jest.fn().mockImplementation(async (key: string, value: string) => {
    store[key] = value;
    return 'OK';
  }),
  zAdd: jest.fn().mockResolvedValue(1),
  hIncrBy: jest.fn().mockImplementation(async (key: string, field: string, increment: number) => {
    statsStore[field] = (statsStore[field] || 0) + increment;
    return statsStore[field];
  }),
  hGetAll: jest.fn().mockImplementation(async (key: string) => {
    const stringStats: Record<string, string> = {};
    for (const field in statsStore) {
      stringStats[field] = statsStore[field].toString();
    }
    return stringStats;
  }),
  zCard: jest.fn().mockResolvedValue(1),
  lPush: jest.fn().mockResolvedValue(1),
  del: jest.fn().mockImplementation(async (key: string) => {
    delete store[key];
    return 1;
  }),
};

jest.mock('../redis', () => ({
  getRedisClient: () => mockRedisClient,
}));

describe('TaskQueue - Stats accuracy on status transitions', () => {
  const queueName = 'default';

  beforeEach(() => {
    for (const key in store) delete store[key];
    statsStore.pending = 0;
    statsStore.processing = 0;
    statsStore.completed = 0;
    statsStore.failed = 0;
    jest.clearAllMocks();
  });

  const createTaskMock = (id: string, status: any): Task => {
    return {
      id,
      name: 'Test Task',
      description: 'Desc',
      priority: 'medium',
      status,
      handler: 'testHandler',
      payload: {},
      retries: 0,
      maxRetries: 3,
      timeout: 30000,
      createdAt: new Date(),
      queue: queueName,
      dependencies: [],
      tags: [],
      metadata: {},
    };
  };

  it('should update stats correctly on task creation and transitions', async () => {
    // 1. Task Creation (increments pending)
    const task = await TaskQueue.createTask('Test Task', 'testHandler', {});
    expect(statsStore.pending).toBe(1);

    // 2. Pending -> Queued (decrements pending)
    await TaskQueue.updateTaskStatus(task.id, 'queued');
    expect(statsStore.pending).toBe(0);
    expect(statsStore.processing).toBe(0);

    // 3. Queued -> Processing (increments processing)
    await TaskQueue.updateTaskStatus(task.id, 'processing');
    expect(statsStore.processing).toBe(1);
    expect(statsStore.pending).toBe(0);

    // 4. Processing -> Completed (decrements processing, increments completed)
    await TaskQueue.updateTaskStatus(task.id, 'completed');
    expect(statsStore.processing).toBe(0);
    expect(statsStore.completed).toBe(1);
  });

  it('should update stats correctly on retry transition', async () => {
    const task = createTaskMock('test-task-1', 'processing');
    store['task:test-task-1'] = JSON.stringify(task);
    statsStore.processing = 1;

    // Retry should decrement processing and change status to retry
    const result = await TaskQueue.retryTask('test-task-1');
    expect(result).toBe(true);
    expect(statsStore.processing).toBe(0);
  });

  it('should update stats correctly when task fails and moves to DLQ', async () => {
    const task = createTaskMock('test-task-2', 'processing');
    task.retries = 3; // at max retries
    store['task:test-task-2'] = JSON.stringify(task);
    statsStore.processing = 1;

    // Retry should move to DLQ (marks failed: decrements processing, increments failed)
    const result = await TaskQueue.retryTask('test-task-2');
    expect(result).toBe(false);
    expect(statsStore.processing).toBe(0);
    expect(statsStore.failed).toBe(1);
  });
});
