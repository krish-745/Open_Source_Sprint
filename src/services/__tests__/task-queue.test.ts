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
    sRem: jest.fn(),
    zRem: jest.fn(),
    sIsMember: jest.fn(),
    zRange: jest.fn(),
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

  describe('Task Grouping (Issue #26)', () => {
    it('should allow task creation with groupId and index it in Redis', async () => {
      redisClient.zCard.mockResolvedValue(0);
      redisClient.zAdd.mockResolvedValue(1);
      redisClient.sAdd.mockResolvedValue(1);

      const task = await TaskQueue.createTask('test-task', 'test-handler', {}, { groupId: 'g1' });

      expect(task).toBeDefined();
      expect(task.groupId).toBe('g1');
      expect(redisClient.sAdd).toHaveBeenCalledWith('group:g1:tasks', task.id);
    });

    it('should retrieve all tasks in a group', async () => {
      redisClient.sMembers.mockResolvedValue(['t1', 't2']);
      redisClient.get.mockImplementation(async (key: string) => {
        if (key === 'task:t1') return JSON.stringify({ id: 't1', name: 'Task 1', groupId: 'g1', status: 'completed' });
        if (key === 'task:t2') return JSON.stringify({ id: 't2', name: 'Task 2', groupId: 'g1', status: 'pending' });
        return null;
      });

      const tasks = await TaskQueue.getGroupTasks('g1');
      expect(tasks.length).toBe(2);
      expect(tasks[0].id).toBe('t1');
      expect(tasks[1].id).toBe('t2');
    });

    it('should calculate group status metrics and aggregate results', async () => {
      redisClient.sMembers.mockResolvedValue(['t1', 't2', 't3']);
      redisClient.get.mockImplementation(async (key: string) => {
        if (key === 'task:t1') return JSON.stringify({ id: 't1', name: 'T1', groupId: 'g1', status: 'completed', result: 'res1' });
        if (key === 'task:t2') return JSON.stringify({ id: 't2', name: 'T2', groupId: 'g1', status: 'failed', error: 'err2' });
        if (key === 'task:t3') return JSON.stringify({ id: 't3', name: 'T3', groupId: 'g1', status: 'processing' });
        return null;
      });

      const status = await TaskQueue.getGroupStatus('g1');
      expect(status).toBeDefined();
      expect(status?.totalTasks).toBe(3);
      expect(status?.completedTasks).toBe(1);
      expect(status?.failedTasks).toBe(1);
      expect(status?.processingTasks).toBe(1);
      expect(status?.completionPercentage).toBeCloseTo(33.33, 1);
      expect(status?.results).toEqual({
        't1': 'res1'
      });
    });

    it('should return null if the group does not exist or has no tasks', async () => {
      redisClient.sMembers.mockResolvedValue([]);
      const status = await TaskQueue.getGroupStatus('g-nonexistent');
      expect(status).toBeNull();
    });
  });

  describe('Secondary Indexing and Optimization', () => {
    it('should index tasks by status and creation time when created', async () => {
      redisClient.zCard.mockResolvedValue(0);
      redisClient.zAdd.mockResolvedValue(1);
      redisClient.sAdd.mockResolvedValue(1);

      const task = await TaskQueue.createTask('test-task', 'test-handler', {});

      expect(redisClient.zAdd).toHaveBeenCalledWith('tasks:created_at', expect.objectContaining({
        score: expect.any(Number),
        value: task.id
      }));
      expect(redisClient.sAdd).toHaveBeenCalledWith('tasks:status:pending', task.id);
      expect(redisClient.sAdd).toHaveBeenCalledWith(`tasks:queue:${task.queue}:status:pending`, task.id);
    });

    it('should transition status indexes when task status updates', async () => {
      const mockTask = {
        id: 'task-idx-1',
        name: 'transition-test',
        status: 'pending',
        queue: 'default',
      };
      redisClient.get.mockResolvedValue(JSON.stringify(mockTask));
      redisClient.sRem.mockResolvedValue(1);
      redisClient.sAdd.mockResolvedValue(1);

      await TaskQueue.updateTaskStatus('task-idx-1', 'processing');

      expect(redisClient.sRem).toHaveBeenCalledWith('tasks:status:pending', 'task-idx-1');
      expect(redisClient.sAdd).toHaveBeenCalledWith('tasks:status:processing', 'task-idx-1');
      expect(redisClient.sRem).toHaveBeenCalledWith('tasks:queue:default:status:pending', 'task-idx-1');
      expect(redisClient.sAdd).toHaveBeenCalledWith('tasks:queue:default:status:processing', 'task-idx-1');
    });

    it('should query creation index and check status sets for cleanupOldTasks', async () => {
      // Setup candidate task IDs
      redisClient.zRange.mockResolvedValue(['t-old-completed', 't-old-pending', 't-old-failed']);
      
      // Mock sIsMember to identify status
      redisClient.sIsMember.mockImplementation(async (key: string, member: string) => {
        if (key === 'tasks:status:completed' && member === 't-old-completed') return true;
        if (key === 'tasks:status:failed' && member === 't-old-failed') return true;
        return false;
      });

      // Mock getTask to return the task structures
      redisClient.get.mockImplementation(async (key: string) => {
        if (key === 'task:t-old-completed') {
          return JSON.stringify({ id: 't-old-completed', queue: 'default', status: 'completed' });
        }
        if (key === 'task:t-old-failed') {
          return JSON.stringify({ id: 't-old-failed', queue: 'default', status: 'failed' });
        }
        return null;
      });

      redisClient.del.mockResolvedValue(1);
      redisClient.sRem.mockResolvedValue(1);
      redisClient.zRem.mockResolvedValue(1);

      const deletedCount = await TaskQueue.cleanupOldTasks(2);

      // Verify the score-based query was made on the creation index
      expect(redisClient.zRange).toHaveBeenCalledWith('tasks:created_at', 0, expect.any(Number), { BY: 'SCORE' });

      // Verify status checks were made
      expect(redisClient.sIsMember).toHaveBeenCalledWith('tasks:status:completed', 't-old-completed');
      expect(redisClient.sIsMember).toHaveBeenCalledWith('tasks:status:failed', 't-old-completed');
      expect(redisClient.sIsMember).toHaveBeenCalledWith('tasks:status:completed', 't-old-pending');
      
      // Completed and failed tasks should be deleted
      expect(redisClient.del).toHaveBeenCalledWith('task:t-old-completed');
      expect(redisClient.del).toHaveBeenCalledWith('task:t-old-failed');
      expect(redisClient.del).not.toHaveBeenCalledWith('task:t-old-pending');

      // Index cleanup
      expect(redisClient.sRem).toHaveBeenCalledWith('tasks:status:completed', 't-old-completed');
      expect(redisClient.zRem).toHaveBeenCalledWith('tasks:created_at', 't-old-completed');
      expect(redisClient.zRem).toHaveBeenCalledWith('tasks:index', 't-old-completed');

      expect(deletedCount).toBe(2);
    });
  });
});
