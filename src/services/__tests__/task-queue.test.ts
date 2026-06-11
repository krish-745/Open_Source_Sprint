import { TaskQueue } from '../task-queue';
import { getRedisClient } from '../redis';
import { Task } from '../../types';

let dlqList: string[] = [];
const store: Record<string, string> = {};
const queueStore: Record<string, string[]> = {};
const listStore: Record<string, string[]> = {};

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
  lPush: jest.fn().mockImplementation(async (key: string, value: string) => {
    if (!listStore[key]) {
      listStore[key] = [];
    }
    listStore[key].push(value);
    return listStore[key].length;
  }),
  lRange: jest.fn().mockImplementation(async (key: string, start: number, stop: number) => {
    return listStore[key] || [];
  }),
  lLen: jest.fn().mockImplementation(async (key: string) => {
    return (listStore[key] || []).length;
  }),
  lRem: jest.fn().mockImplementation(async (key: string, count: number, value: string) => {
    const list = listStore[key] || [];
    const idx = list.indexOf(value);
    if (idx !== -1) {
      list.splice(idx, 1);
      return 1;
    }
    return 0;
  }),
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
    dlqList = [];
    for (const key in store) delete store[key];
    for (const key in queueStore) delete queueStore[key];
    for (const key in listStore) delete listStore[key];
    listStore['dlq:tasks'] = dlqList;
    jest.clearAllMocks();
  });

  describe('DLQ Inspection and Management', () => {
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
      expect(dlqList.length).toBe(1);
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
