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

  describe('createTask payload validation', () => {
    it('should create a task with a valid object payload', async () => {
      const payload = { key: 'value' };
      const task = await TaskQueue.createTask('Test Task', 'testHandler', payload);
      
      expect(task).toBeDefined();
      expect(task.payload).toEqual(payload);
    });

    it('should default null payload to an empty object', async () => {
      const task = await TaskQueue.createTask('Test Task', 'testHandler', null);
      
      expect(task).toBeDefined();
      expect(task.payload).toEqual({});
    });

    it('should default undefined payload to an empty object', async () => {
      const task = await TaskQueue.createTask('Test Task', 'testHandler', undefined);
      
      expect(task).toBeDefined();
      expect(task.payload).toEqual({});
    });

    it('should throw an error if payload is a string', async () => {
      await expect(
        TaskQueue.createTask('Test Task', 'testHandler', 'invalid-string' as any)
      ).rejects.toThrow('Payload must be a valid object');
    });

    it('should throw an error if payload is an array', async () => {
      await expect(
        TaskQueue.createTask('Test Task', 'testHandler', [1, 2, 3] as any)
      ).rejects.toThrow('Payload must be a valid object');
    });

    it('should throw an error if payload is a number', async () => {
      await expect(
        TaskQueue.createTask('Test Task', 'testHandler', 123 as any)
      ).rejects.toThrow('Payload must be a valid object');
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
