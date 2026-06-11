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
});
