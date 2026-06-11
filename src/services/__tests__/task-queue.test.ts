import { TaskQueue } from '../task-queue';
import { getRedisClient } from '../redis';
import { Task } from '../../types';

const store: Record<string, string> = {};
const queueStore: Record<string, string[]> = {};

const mockRedisClient = {
  get: jest.fn().mockImplementation(async (key: string) => store[key] || null),
  set: jest.fn().mockImplementation(async (key: string, value: string) => {
    store[key] = value;
    return 'OK';
  }),
  zAdd: jest.fn().mockImplementation(async (key: string, item: { score: number; value: string }) => {
    if (!queueStore[key]) {
      queueStore[key] = [];
    }
    queueStore[key] = queueStore[key].filter(v => v !== item.value);
    queueStore[key].push(item.value);
    return 1;
  }),
  zCard: jest.fn().mockImplementation(async (key: string) => {
    return (queueStore[key] || []).length;
  }),
  zRange: jest.fn().mockImplementation(async (key: string, start: number, stop: number, options?: any) => {
    const list = queueStore[key] || [];
    if (start === 0 && stop === -1) {
      return list;
    }
    const end = stop < 0 ? list.length : stop + 1;
    return list.slice(start, end);
  }),
  hIncrBy: jest.fn().mockResolvedValue(1),
  lPush: jest.fn().mockResolvedValue(1),
  del: jest.fn().mockImplementation(async (key: string) => {
    delete store[key];
    return 1;
  }),
};

jest.mock('../redis', () => ({
  getRedisClient: () => mockRedisClient,
}));

describe('TaskQueue Tests', () => {
  beforeEach(() => {
    for (const key in store) delete store[key];
    for (const key in queueStore) delete queueStore[key];
    jest.clearAllMocks();
  });

  describe('getQueueTasks with filtering', () => {
    const queueName = 'test-queue';
    const queueKey = `queue:${queueName}`;

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

      const resultSingle = await TaskQueue.getQueueTasks(queueName, 10, 0, { tags: 'cpu' });
      expect(resultSingle.length).toBe(2);
      expect(resultSingle.map(t => t.id)).toEqual(['1', '3']);

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

  describe('retryTask (Fix #18)', () => {
    it('should strictly delete the error field, not set it to undefined', async () => {
      const mockTask = {
        id: 'task-1',
        name: 'failing-task',
        status: 'failed',
        retries: 0,
        maxRetries: 3,
        error: 'Connection timed out',
        queue: 'default',
        priority: 'medium',
      };

      store['task:task-1'] = JSON.stringify(mockTask);

      await TaskQueue.retryTask('task-1');

      const savedTask = JSON.parse(store['task:task-1']);
      expect(Object.keys(savedTask)).not.toContain('error');
      expect(savedTask.error).toBeUndefined();
    });

    it('should increment retries and set status to retry', async () => {
      const mockTask = {
        id: 'task-2',
        name: 'failing-task',
        status: 'failed',
        retries: 1,
        maxRetries: 3,
        error: 'Handler crashed',
        queue: 'default',
        priority: 'high',
      };

      store['task:task-2'] = JSON.stringify(mockTask);

      await TaskQueue.retryTask('task-2');

      const savedTask = JSON.parse(store['task:task-2']);
      expect(savedTask.status).toBe('retry');
      expect(savedTask.retries).toBe(2);
    });

    it('should move to dead letter queue and return false when maxRetries is exhausted', async () => {
      const mockTask = {
        id: 'task-3',
        name: 'failing-task',
        status: 'failed',
        retries: 3,
        maxRetries: 3,
        error: 'Permanent failure',
        queue: 'default',
        priority: 'low',
      };

      store['task:task-3'] = JSON.stringify(mockTask);

      const result = await TaskQueue.retryTask('task-3');

      expect(result).toBe(false);
      expect(store['task:task-3']).toBeUndefined();
    });
  });

  describe('createTask (Fix #22)', () => {
    it('should reject new tasks when queue size exceeds MAX_QUEUE_SIZE limit', async () => {
      process.env.MAX_QUEUE_SIZE = '100';
      const zCardSpy = jest.spyOn(mockRedisClient, 'zCard').mockResolvedValueOnce(100);
      
      await expect(
        TaskQueue.createTask('test-task', 'test-handler', {})
      ).rejects.toThrow(/Queue default exceeds maximum size of 100/i);

      zCardSpy.mockRestore();
    });

    it('should allow task creation when queue size is below MAX_QUEUE_SIZE limit', async () => {
      process.env.MAX_QUEUE_SIZE = '100';
      const zCardSpy = jest.spyOn(mockRedisClient, 'zCard').mockResolvedValueOnce(99);

      const task = await TaskQueue.createTask('test-task', 'test-handler', {});
      
      expect(task).toBeDefined();
      expect(task.name).toBe('test-task');

      zCardSpy.mockRestore();
    });
  });
});
