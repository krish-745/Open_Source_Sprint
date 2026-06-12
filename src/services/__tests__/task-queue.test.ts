import { TaskQueue, QueueFullError } from '../task-queue';
import { getRedisClient } from '../redis';
import { Task } from '../../types';

const store: Record<string, string> = {};
const queueStore: Record<string, string[]> = {};

const mockRedisClient = {
  watch: jest.fn().mockResolvedValue('OK'),
  unwatch: jest.fn().mockResolvedValue('OK'),
  multi: jest.fn().mockImplementation(() => {
    const m: any = {
      set: jest.fn().mockImplementation((key: string, value: string) => {
        store[key] = value;
        return m;
      }),
      exec: jest.fn().mockResolvedValue([['OK']]),
    };
    return m;
  }),
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
  zRem: jest.fn().mockImplementation(async (key: string, value: string) => {
    if (queueStore[key]) {
      queueStore[key] = queueStore[key].filter(v => v !== value);
    }
    return 1;
  }),
  hGetAll: jest.fn().mockResolvedValue({}),
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
      const task = await TaskQueue.createTask('Test Task', 'testHandler', null as any);
      
      expect(task).toBeDefined();
      expect(task.payload).toEqual({});
    });

    it('should default undefined payload to an empty object', async () => {
      const task = await TaskQueue.createTask('Test Task', 'testHandler', undefined as any);
      
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

  describe('createTask', () => {
    it('should create a task with default options', async () => {
      const task = await TaskQueue.createTask('test-task', 'test-handler', { foo: 'bar' });
      expect(task).toBeDefined();
      expect(task.name).toBe('test-task');
      expect(task.handler).toBe('test-handler');
      expect(task.payload).toEqual({ foo: 'bar' });
      expect(task.queue).toBe('default');
      expect(task.priority).toBe('medium');
      expect(task.status).toBe('pending');
      
      expect(mockRedisClient.set).toHaveBeenCalledTimes(1);
      expect(mockRedisClient.zAdd).toHaveBeenCalledTimes(2); // One for task index, one for queue
      expect(mockRedisClient.hIncrBy).toHaveBeenCalledWith('queue:default:stats', 'pending', 1);
    });

    it('should create a task with custom options', async () => {
      const scheduledFor = new Date();
      const task = await TaskQueue.createTask('test-task', 'test-handler', {}, {
        queueName: 'custom-queue',
        priority: 'high',
        maxRetries: 5,
        timeout: 10000,
        dependencies: ['dep-1'],
        scheduledFor,
        tags: ['tag1'],
      });

      expect(task.queue).toBe('custom-queue');
      expect(task.priority).toBe('high');
      expect(task.maxRetries).toBe(5);
      expect(task.timeout).toBe(10000);
      expect(task.dependencies).toEqual(['dep-1']);
      expect(task.scheduledFor).toBe(scheduledFor);
      expect(task.tags).toEqual(['tag1']);
    });

    it('should reject new tasks when queue size exceeds MAX_QUEUE_SIZE limit', async () => {
      process.env.MAX_QUEUE_SIZE = '100';
      mockRedisClient.zCard.mockResolvedValueOnce(100);
      
      await expect(
        TaskQueue.createTask('test-task', 'test-handler', {})
      ).rejects.toThrow(/Queue default exceeds maximum size of 100/i);
    });

    it('should allow task creation when queue size is below MAX_QUEUE_SIZE limit', async () => {
      process.env.MAX_QUEUE_SIZE = '100';
      mockRedisClient.zCard.mockResolvedValueOnce(99);

      const task = await TaskQueue.createTask('test-task', 'test-handler', {});
      expect(task).toBeDefined();
      expect(task.name).toBe('test-task');
    });
  });

  describe('createTasksBatch', () => {
    it('should throw error if batch is empty', async () => {
      await expect(TaskQueue.createTasksBatch([])).rejects.toThrow('Batch must contain at least one task');
    });

    it('should throw error if batch exceeds 1000 tasks', async () => {
      const inputs = Array(1001).fill({ name: 'n', handler: 'h' });
      await expect(TaskQueue.createTasksBatch(inputs)).rejects.toThrow('Batch size exceeds the maximum of 1000 tasks');
    });

    it('should throw error if any task is invalid', async () => {
      const inputs = [{ name: 'task-1', handler: 'h' }, { name: '', handler: 'h' }];
      await expect(TaskQueue.createTasksBatch(inputs)).rejects.toThrow('Invalid task at index 1: name and handler are required');
    });

    it('should create multiple tasks', async () => {
      const inputs = [
        { name: 'task-1', handler: 'handler-1', payload: { a: 1 } },
        { name: 'task-2', handler: 'handler-2' }
      ];
      const tasks = await TaskQueue.createTasksBatch(inputs);
      expect(tasks).toHaveLength(2);
      expect(tasks[0].name).toBe('task-1');
      expect(tasks[1].name).toBe('task-2');
      expect(mockRedisClient.set).toHaveBeenCalledTimes(2);
    });
  });

  describe('getTask', () => {
    it('should return task if it exists', async () => {
      const mockTask = { id: 'task-1', name: 'test' };
      store['task:task-1'] = JSON.stringify(mockTask);

      const task = await TaskQueue.getTask('task-1');
      expect(task).toEqual(mockTask);
      expect(mockRedisClient.get).toHaveBeenCalledWith('task:task-1');
    });

    it('should return null if task does not exist', async () => {
      const task = await TaskQueue.getTask('task-missing');
      expect(task).toBeNull();
    });
  });

  describe('updateTaskStatus', () => {
    it('should throw error if task not found', async () => {
      await expect(TaskQueue.updateTaskStatus('missing', 'completed')).rejects.toThrow('Task missing not found');
    });

    it('should update status to processing and set startedAt', async () => {
      const mockTask = { id: 'task-1', status: 'pending', queue: 'default' };
      store['task:task-1'] = JSON.stringify(mockTask);

      await TaskQueue.updateTaskStatus('task-1', 'processing');

      const savedTask = JSON.parse(store['task:task-1']);
      expect(savedTask.status).toBe('processing');
      expect(savedTask.startedAt).toBeDefined();

      expect(mockRedisClient.hIncrBy).toHaveBeenCalledWith('queue:default:stats', 'pending', -1);
      expect(mockRedisClient.hIncrBy).toHaveBeenCalledWith('queue:default:stats', 'processing', 1);
    });

    it('should update status to completed and set completedAt', async () => {
      const mockTask = { id: 'task-1', status: 'processing', queue: 'default' };
      store['task:task-1'] = JSON.stringify(mockTask);

      await TaskQueue.updateTaskStatus('task-1', 'completed', { result: 'success' });

      const savedTask = JSON.parse(store['task:task-1']);
      expect(savedTask.status).toBe('completed');
      expect(savedTask.completedAt).toBeDefined();
      expect(savedTask.result).toBe('success');

      expect(mockRedisClient.hIncrBy).toHaveBeenCalledWith('queue:default:stats', 'processing', -1);
      expect(mockRedisClient.hIncrBy).toHaveBeenCalledWith('queue:default:stats', 'completed', 1);
    });
  });

  describe('getNextTask', () => {
    it('should return null if queue is empty', async () => {
      const task = await TaskQueue.getNextTask('default');
      expect(task).toBeNull();
    });

    it('should skip tasks with unmet dependencies', async () => {
      const task1 = { id: 't1', dependencies: ['d1'] }; // d1 not found
      queueStore['queue:default'] = ['t1'];
      store['task:t1'] = JSON.stringify(task1);

      const task = await TaskQueue.getNextTask('default');
      expect(task).toBeNull();
    });

    it('should skip tasks scheduled for the future', async () => {
      const futureDate = new Date();
      futureDate.setHours(futureDate.getHours() + 1);
      const task1 = { id: 't1', dependencies: [], scheduledFor: futureDate.toISOString() };
      
      queueStore['queue:default'] = ['t1'];
      store['task:t1'] = JSON.stringify(task1);

      const task = await TaskQueue.getNextTask('default');
      expect(task).toBeNull();
    });

    it('should return first runnable task', async () => {
      const task1 = { id: 't1', dependencies: [], status: 'pending' };
      queueStore['queue:default'] = ['t1'];
      store['task:t1'] = JSON.stringify(task1);

      const task = await TaskQueue.getNextTask('default');
      expect(task).toEqual(task1);
    });
  });

  describe('retryTask (Fix #18)', () => {
    it('should strictly delete the error field, not set it to undefined', async () => {
      const mockTask = {
        id: 'task-1', name: 'failing-task', status: 'failed', retries: 0, maxRetries: 3,
        error: 'Connection timed out', queue: 'default', priority: 'medium',
      };
      store['task:task-1'] = JSON.stringify(mockTask);

      await TaskQueue.retryTask('task-1');

      const savedTask = JSON.parse(store['task:task-1']);
      expect(Object.keys(savedTask)).not.toContain('error');
      expect(savedTask.error).toBeUndefined();
    });

    it('should increment retries and set status to retry', async () => {
      const mockTask = {
        id: 'task-2', name: 'failing-task', status: 'failed', retries: 1, maxRetries: 3,
        error: 'Handler crashed', queue: 'default', priority: 'high',
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
      expect(mockRedisClient.lPush).toHaveBeenCalled();
    });
  });

  describe('getQueueTasks', () => {
    it('should return tasks for given queue', async () => {
      queueStore['queue:default'] = ['t1', 't2'];
      store['task:t1'] = JSON.stringify({ id: 't1' });
      store['task:t2'] = JSON.stringify({ id: 't2' });

      const tasks = await TaskQueue.getQueueTasks('default', 10, 0);
      expect(tasks).toHaveLength(2);
      expect(tasks[0].id).toBe('t1');
      expect(tasks[1].id).toBe('t2');
    });
  });

  describe('getQueueStats', () => {
    it('should return queue statistics', async () => {
      mockRedisClient.zCard.mockResolvedValueOnce(5);
      mockRedisClient.hGetAll.mockResolvedValueOnce({
        pending: '2', processing: '1', completed: '2', failed: '0'
      });

      const stats = await TaskQueue.getQueueStats('default');
      expect(stats).toEqual({
        queueName: 'default', queueSize: 5, pending: 2, processing: 1, completed: 2, failed: 0
      });
    });
  });

  describe('cleanupOldTasks', () => {
    it('should delete old completed and failed tasks', async () => {
      queueStore['tasks:index'] = ['t1', 't2', 't3'];
      store['task:t1'] = JSON.stringify({ id: 't1', status: 'completed' });
      store['task:t2'] = JSON.stringify({ id: 't2', status: 'pending' });
      store['task:t3'] = JSON.stringify({ id: 't3', status: 'failed' });

      mockRedisClient.zRange.mockResolvedValueOnce(['t1', 't2', 't3']);

      const deleted = await TaskQueue.cleanupOldTasks(24);
      expect(deleted).toBe(2);
      expect(store['task:t1']).toBeUndefined();
      expect(store['task:t3']).toBeUndefined();
      expect(queueStore['tasks:index']).not.toContain('t1');
      expect(queueStore['tasks:index']).not.toContain('t3');
    });
  });

  describe('recoverStaleTasks', () => {
    it('should recover processing tasks older than staleMs', async () => {
      const oldTime = new Date(Date.now() - 10 * 60 * 1000).toISOString();
      const recentTime = new Date().toISOString();

      queueStore['tasks:index'] = ['t1', 't2'];
      store['task:t1'] = JSON.stringify({ 
        id: 't1', status: 'processing', startedAt: oldTime, workerId: 'w1', queue: 'default', priority: 'medium'
      });
      store['task:t2'] = JSON.stringify({ 
        id: 't2', status: 'processing', startedAt: recentTime, workerId: 'w2' 
      });

      mockRedisClient.zRange.mockResolvedValueOnce(['t1', 't2']);

      const recovered = await TaskQueue.recoverStaleTasks(5 * 60 * 1000);
      
      expect(recovered).toBe(1);
      
      const savedTask1 = JSON.parse(store['task:t1']);
      expect(savedTask1.status).toBe('queued');
      expect(savedTask1.workerId).toBeUndefined();
      expect(mockRedisClient.zAdd).toHaveBeenCalledWith('queue:default', expect.any(Object));
    });
  });

  describe('evaluateBranches', () => {
    it('should return empty array if no branches', () => {
      const task = { id: 't1', name: 't', handler: 'h', payload: {} } as any;
      expect(TaskQueue.evaluateBranches(task, 'success')).toEqual([]);
    });

    it('should match substring condition', () => {
      const task = { 
        id: 't1', name: 't', handler: 'h', payload: {},
        branches: [
          { condition: 'success', taskName: 'next-t1', payloadTemplate: {} },
          { condition: 'fail', taskName: 'next-t2', payloadTemplate: {} }
        ]
      } as any;
      const branches = TaskQueue.evaluateBranches(task, 'operation success done');
      expect(branches).toHaveLength(1);
      expect(branches[0].condition).toBe('success');
    });

    it('should match regex condition', () => {
      const task = { 
        id: 't1', name: 't', handler: 'h', payload: {},
        branches: [{ condition: 'regex:^\\d+$', taskName: 'next-t1', payloadTemplate: {} }]
      } as any;
      const branches = TaskQueue.evaluateBranches(task, '12345');
      expect(branches).toHaveLength(1);
    });
  });

  describe('getNextBatch', () => {
    it('should return empty array if batchSize < 1', async () => {
      expect(await TaskQueue.getNextBatch('default', 0)).toEqual([]);
    });

    it('should return batch of tasks', async () => {
      queueStore['queue:default'] = ['t1', 't2'];
      store['task:t1'] = JSON.stringify({ id: 't1', dependencies: [] });
      store['task:t2'] = JSON.stringify({ id: 't2', dependencies: [] });

      const batch = await TaskQueue.getNextBatch('default', 2);
      expect(batch).toHaveLength(2);
      expect(batch.map(t => t.id)).toEqual(['t1', 't2']);
    });
  });

  describe('cancelTask', () => {
    it('should throw error if task not found', async () => {
      await expect(TaskQueue.cancelTask('missing')).rejects.toThrow('Task missing not found');
    });

    it('should return false if task is already completed', async () => {
      store['task:t1'] = JSON.stringify({ id: 't1', status: 'completed' });
      expect(await TaskQueue.cancelTask('t1')).toBe(false);
    });

    it('should cancel pending task', async () => {
      store['task:t1'] = JSON.stringify({ id: 't1', status: 'pending', queue: 'default' });
      queueStore['queue:default'] = ['t1'];
      
      const result = await TaskQueue.cancelTask('t1');
      expect(result).toBe(true);
      
      const savedTask = JSON.parse(store['task:t1']);
      expect(savedTask.status).toBe('cancelled');
      expect(queueStore['queue:default']).not.toContain('t1');
    });
  });

  describe('requeueTask', () => {
    it('should return false if task not found', async () => {
      expect(await TaskQueue.requeueTask('missing')).toBe(false);
    });

    it('should requeue a processing task', async () => {
      store['task:t1'] = JSON.stringify({ id: 't1', status: 'processing', workerId: 'w1', queue: 'default', priority: 'medium' });
      
      const result = await TaskQueue.requeueTask('t1');
      expect(result).toBe(true);
      
      const savedTask = JSON.parse(store['task:t1']);
      expect(savedTask.status).toBe('queued');
      expect(savedTask.workerId).toBeUndefined();
      expect(mockRedisClient.zAdd).toHaveBeenCalledWith('queue:default', expect.any(Object));
    });
  });
});
