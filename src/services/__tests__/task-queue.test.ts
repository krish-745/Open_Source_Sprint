import { TaskQueue } from '../task-queue';
import { getRedisClient } from '../redis';

jest.mock('../redis', () => {
  const mClient = {
    get: jest.fn(),
    set: jest.fn(),
    zAdd: jest.fn(),
    zCard: jest.fn(),
    hIncrBy: jest.fn(),
    lPush: jest.fn(),
    del: jest.fn(),
    sAdd: jest.fn(),
    sMembers: jest.fn(),
    expire: jest.fn(),
  };
  return { getRedisClient: jest.fn(() => mClient) };
});

describe('TaskQueue', () => {
  let redisClient: any;

  beforeEach(() => {
    redisClient = getRedisClient();
    jest.clearAllMocks();
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

      redisClient.get.mockResolvedValue(JSON.stringify(mockTask));

      await TaskQueue.retryTask('task-1');

      const setCall = redisClient.set.mock.calls[0];
      expect(setCall).toBeDefined();

      const savedTask = JSON.parse(setCall[1]);

      // error must be completely absent from the serialized object
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

      redisClient.get.mockResolvedValue(JSON.stringify(mockTask));

      await TaskQueue.retryTask('task-2');

      const savedTask = JSON.parse(redisClient.set.mock.calls[0][1]);
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

      redisClient.get.mockResolvedValue(JSON.stringify(mockTask));

      const result = await TaskQueue.retryTask('task-3');

      expect(result).toBe(false);
      expect(redisClient.set).not.toHaveBeenCalled();
    });
  });

  describe('createTask (Fix #22)', () => {
    it('should reject new tasks when queue size exceeds MAX_QUEUE_SIZE limit', async () => {
      // Configure max size to 100
      process.env.MAX_QUEUE_SIZE = '100';
      
      // Simulate queue already having 100 tasks
      redisClient.zCard.mockResolvedValue(100);
      
      await expect(
        TaskQueue.createTask('test-task', 'test-handler', {})
      ).rejects.toThrow(/Queue default exceeds maximum size of 100/i);
    });

    it('should allow task creation when queue size is below MAX_QUEUE_SIZE limit', async () => {
      process.env.MAX_QUEUE_SIZE = '100';
      redisClient.zCard.mockResolvedValue(99);
      redisClient.zAdd.mockResolvedValue(1); // Mock adding to set

      const task = await TaskQueue.createTask('test-task', 'test-handler', {});
      
      expect(task).toBeDefined();
      expect(task.name).toBe('test-task');
    });
  });

  describe('Distributed Tracing (Issue #39)', () => {
    it('should generate a traceId when no traceId is provided and no dependencies', async () => {
      redisClient.zCard.mockResolvedValue(0);
      const task = await TaskQueue.createTask('test-task', 'handler', {});

      expect(task.traceId).toBeDefined();
      expect(typeof task.traceId).toBe('string');
      // No dependencies → no parentSpanId
      expect(task.parentSpanId).toBeUndefined();
      expect(redisClient.sAdd).toHaveBeenCalledWith(`trace:${task.traceId}`, task.id);
      // Trace index must be given a TTL so it doesn't accumulate forever
      expect(redisClient.expire).toHaveBeenCalledWith(`trace:${task.traceId}`, expect.any(Number));
    });

    it('should inherit traceId and set parentSpanId from the dependency task', async () => {
      const depTask = {
        id: 'dep-task-id',
        status: 'completed',
        traceId: 'trace-1234',
      };

      redisClient.get.mockResolvedValue(JSON.stringify(depTask));
      redisClient.zCard.mockResolvedValue(0);

      const task = await TaskQueue.createTask('child-task', 'handler', {}, {
        dependencies: ['dep-task-id'],
      });

      // traceId flows down from the dependency
      expect(task.traceId).toBe('trace-1234');
      // parentSpanId points at the direct dependency, enabling call-tree reconstruction
      expect(task.parentSpanId).toBe('dep-task-id');
      expect(redisClient.sAdd).toHaveBeenCalledWith('trace:trace-1234', task.id);
      expect(redisClient.expire).toHaveBeenCalledWith('trace:trace-1234', expect.any(Number));
    });

    it('should use an explicitly provided traceId and parentSpanId over auto-derived values', async () => {
      const depTask = { id: 'dep-id', status: 'completed', traceId: 'trace-auto' };
      redisClient.get.mockResolvedValue(JSON.stringify(depTask));
      redisClient.zCard.mockResolvedValue(0);

      const task = await TaskQueue.createTask('override-task', 'handler', {}, {
        dependencies: ['dep-id'],
        traceId: 'trace-explicit',
        parentSpanId: 'custom-parent-span',
      });

      expect(task.traceId).toBe('trace-explicit');
      expect(task.parentSpanId).toBe('custom-parent-span');
    });

    it('should query all tasks in a trace by traceId', async () => {
      redisClient.sMembers.mockResolvedValue(['task-1', 'task-2']);
      redisClient.get.mockImplementation((key: string) => {
        if (key === 'task:task-1') return Promise.resolve(JSON.stringify({ id: 'task-1', traceId: 'trace-999' }));
        if (key === 'task:task-2') return Promise.resolve(JSON.stringify({ id: 'task-2', traceId: 'trace-999' }));
        return Promise.resolve(null);
      });

      const tasks = await TaskQueue.getTasksByTraceId('trace-999');
      expect(tasks).toHaveLength(2);
      expect(tasks.map(t => t.id)).toEqual(['task-1', 'task-2']);
    });

    it('should return empty array when traceId has no registered tasks', async () => {
      redisClient.sMembers.mockResolvedValue([]);
      const tasks = await TaskQueue.getTasksByTraceId('trace-no-match');
      expect(tasks).toHaveLength(0);
    });
  });
});
