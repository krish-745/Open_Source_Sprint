import { TaskQueue } from '../task-queue';
import { getRedisClient } from '../redis';
import { Task } from '../../types';

// Mock the Redis client using an in-memory store
const store: Record<string, string> = {};
const queueStore: Record<string, string[]> = {};

const mockRedisClient = {
  get: jest.fn().mockImplementation(async (key: string) => store[key] || null),
  zRange: jest.fn().mockImplementation(async (key: string, start: number, stop: number, options?: any) => {
    const list = queueStore[key] || [];
    if (start === 0 && stop === -1) {
      return list;
    }
    const end = stop < 0 ? list.length : stop + 1;
    return list.slice(start, end);
  }),
};

jest.mock('../redis', () => ({
  getRedisClient: () => mockRedisClient,
}));

describe('TaskQueue - getQueueTasks with filtering', () => {
  const queueName = 'test-queue';
  const queueKey = `queue:${queueName}`;

  beforeEach(() => {
    for (const key in store) delete store[key];
    for (const key in queueStore) delete queueStore[key];
    jest.clearAllMocks();
  });

  const createTaskMock = (id: string, overrides: Partial<Task>): Task => {
    return {
      id,
      name: `Task ${id}`,
      description: `Description ${id}`,
      priority: 'medium',
      status: 'pending',
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
      ...overrides,
    };
  };

  it('should return all tasks when no filters are provided', async () => {
    const task1 = createTaskMock('1', { status: 'pending' });
    const task2 = createTaskMock('2', { status: 'processing' });
    
    store['task:1'] = JSON.stringify(task1);
    store['task:2'] = JSON.stringify(task2);
    queueStore[queueKey] = ['1', '2'];

    const result = await TaskQueue.getQueueTasks(queueName);
    expect(result.length).toBe(2);
  });

  it('should filter tasks by status', async () => {
    const task1 = createTaskMock('1', { status: 'pending' });
    const task2 = createTaskMock('2', { status: 'processing' });
    const task3 = createTaskMock('3', { status: 'completed' });
    
    store['task:1'] = JSON.stringify(task1);
    store['task:2'] = JSON.stringify(task2);
    store['task:3'] = JSON.stringify(task3);
    queueStore[queueKey] = ['1', '2', '3'];

    const result = await TaskQueue.getQueueTasks(queueName, 10, 0, { status: 'processing' });
    expect(result.length).toBe(1);
    expect(result[0].id).toBe('2');

    const resultMulti = await TaskQueue.getQueueTasks(queueName, 10, 0, { status: ['pending', 'completed'] });
    expect(resultMulti.length).toBe(2);
    expect(resultMulti.map(t => t.id)).toEqual(['1', '3']);
  });

  it('should filter tasks by priority', async () => {
    const task1 = createTaskMock('1', { priority: 'low' });
    const task2 = createTaskMock('2', { priority: 'high' });
    
    store['task:1'] = JSON.stringify(task1);
    store['task:2'] = JSON.stringify(task2);
    queueStore[queueKey] = ['1', '2'];

    const result = await TaskQueue.getQueueTasks(queueName, 10, 0, { priority: 'high' });
    expect(result.length).toBe(1);
    expect(result[0].id).toBe('2');
  });

  it('should filter tasks by tags matching all required tags', async () => {
    const task1 = createTaskMock('1', { tags: ['cpu', 'fast'] });
    const task2 = createTaskMock('2', { tags: ['io', 'slow'] });
    const task3 = createTaskMock('3', { tags: ['cpu', 'slow'] });
    
    store['task:1'] = JSON.stringify(task1);
    store['task:2'] = JSON.stringify(task2);
    store['task:3'] = JSON.stringify(task3);
    queueStore[queueKey] = ['1', '2', '3'];

    // Filter by single tag
    const resultSingle = await TaskQueue.getQueueTasks(queueName, 10, 0, { tags: 'cpu' });
    expect(resultSingle.length).toBe(2);
    expect(resultSingle.map(t => t.id)).toEqual(['1', '3']);

    // Filter by multiple tags (AND logic)
    const resultMulti = await TaskQueue.getQueueTasks(queueName, 10, 0, { tags: ['cpu', 'slow'] });
    expect(resultMulti.length).toBe(1);
    expect(resultMulti[0].id).toBe('3');
  });

  it('should filter tasks by date range', async () => {
    const date1 = new Date('2026-06-01T00:00:00Z');
    const date2 = new Date('2026-06-05T00:00:00Z');
    const date3 = new Date('2026-06-10T00:00:00Z');

    const task1 = createTaskMock('1', { createdAt: date1 });
    const task2 = createTaskMock('2', { createdAt: date2 });
    const task3 = createTaskMock('3', { createdAt: date3 });
    
    store['task:1'] = JSON.stringify(task1);
    store['task:2'] = JSON.stringify(task2);
    store['task:3'] = JSON.stringify(task3);
    queueStore[queueKey] = ['1', '2', '3'];

    const result = await TaskQueue.getQueueTasks(queueName, 10, 0, {
      startDate: new Date('2026-06-03T00:00:00Z'),
      endDate: new Date('2026-06-07T00:00:00Z'),
    });
    expect(result.length).toBe(1);
    expect(result[0].id).toBe('2');
  });

  it('should combine multiple filters with AND logic', async () => {
    const task1 = createTaskMock('1', { status: 'pending', priority: 'high' });
    const task2 = createTaskMock('2', { status: 'processing', priority: 'high' });
    const task3 = createTaskMock('3', { status: 'pending', priority: 'low' });
    
    store['task:1'] = JSON.stringify(task1);
    store['task:2'] = JSON.stringify(task2);
    store['task:3'] = JSON.stringify(task3);
    queueStore[queueKey] = ['1', '2', '3'];

    const result = await TaskQueue.getQueueTasks(queueName, 10, 0, {
      status: 'pending',
      priority: 'high',
    });
    expect(result.length).toBe(1);
    expect(result[0].id).toBe('1');
  });
});
