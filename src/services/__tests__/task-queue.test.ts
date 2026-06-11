import { TaskQueue } from '../task-queue';
import { getRedisClient } from '../redis';
import { Task } from '../../types';

const store: Record<string, string> = {};
const queueStore: Record<string, string[]> = {};
let taskIdsIndex: string[] = [];

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
    if (key === 'tasks:index') {
      return taskIdsIndex;
    }
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
    taskIdsIndex = [];
    jest.clearAllMocks();
  });

  describe('searchTasks', () => {
    const createTaskMock = (id: string, name: string, description: string): Task => {
      return {
        id,
        name,
        description,
        priority: 'medium',
        status: 'pending',
        handler: 'testHandler',
        payload: {},
        retries: 0,
        maxRetries: 3,
        timeout: 30000,
        createdAt: new Date(),
        queue: 'default',
        dependencies: [],
        tags: [],
        metadata: {},
      };
    };

    it('should return empty array when query is empty', async () => {
      const results = await TaskQueue.searchTasks('');
      expect(results).toEqual([]);
    });

    it('should find tasks by case-insensitive name and description and score them', async () => {
      const task1 = createTaskMock('1', 'Report Generator', 'Generates weekly sales reports');
      const task2 = createTaskMock('2', 'Email Dispatcher', 'Sends reports via email');
      const task3 = createTaskMock('3', 'Database Backup', 'Backs up sales database');

      store['task:1'] = JSON.stringify(task1);
      store['task:2'] = JSON.stringify(task2);
      store['task:3'] = JSON.stringify(task3);
      taskIdsIndex = ['1', '2', '3'];

      const results = await TaskQueue.searchTasks('report');
      
      expect(results.length).toBe(2);
      expect(results[0].taskId).toBe('1');
      expect(results[0].score).toBe(7);
      expect(results[1].taskId).toBe('2');
      expect(results[1].score).toBe(2);
    });

    it('should rank exact name match highest', async () => {
      const task1 = createTaskMock('1', 'Backup', 'Backup description');
      const task2 = createTaskMock('2', 'Database Backup Service', 'Database backup description');

      store['task:1'] = JSON.stringify(task1);
      store['task:2'] = JSON.stringify(task2);
      taskIdsIndex = ['1', '2'];

      const results = await TaskQueue.searchTasks('Backup');
      expect(results.length).toBe(2);
      expect(results[0].taskId).toBe('1'); // exact name match
      expect(results[1].taskId).toBe('2'); // partial name match
    });

    it('should handle special characters safely without crashing', async () => {
      const task = createTaskMock('1', 'Special $&* Task', 'Contains special chars');
      store['task:1'] = JSON.stringify(task);
      taskIdsIndex = ['1'];

      const results = await TaskQueue.searchTasks('$&*');
      expect(results.length).toBe(1);
      expect(results[0].taskId).toBe('1');
    });

    it('should respect the limit parameter', async () => {
      const task1 = createTaskMock('1', 'Report Generator 1', 'Desc');
      const task2 = createTaskMock('2', 'Report Generator 2', 'Desc');

      store['task:1'] = JSON.stringify(task1);
      store['task:2'] = JSON.stringify(task2);
      taskIdsIndex = ['1', '2'];

      const results = await TaskQueue.searchTasks('Report', 1);
      expect(results.length).toBe(1);
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
