import { TaskQueue, CircularDependencyError } from '../task-queue';
import { getRedisClient } from '../redis';
import { v4 as uuidv4 } from 'uuid';

jest.mock('../redis');
jest.mock('uuid', () => {
  const actualUuid = jest.requireActual('uuid');
  return {
    v4: jest.fn().mockImplementation(() => actualUuid.v4()),
  };
});

describe('TaskQueue Tests', () => {
  let mockStore: Record<string, string> = {};
  let mockSortedSets: Record<string, Array<{ score: number; value: string }>> = {};
  let mockHashes: Record<string, Record<string, string>> = {};

  const mockRedisClient = {
    set: jest.fn().mockImplementation(async (key: string, value: string) => {
      mockStore[key] = value;
      return 'OK';
    }),
    get: jest.fn().mockImplementation(async (key: string) => {
      return mockStore[key] || null;
    }),
    zAdd: jest.fn().mockImplementation(async (key: string, item: { score: number; value: string }) => {
      if (!mockSortedSets[key]) {
        mockSortedSets[key] = [];
      }
      mockSortedSets[key] = mockSortedSets[key].filter(i => i.value !== item.value);
      mockSortedSets[key].push(item);
      mockSortedSets[key].sort((a, b) => a.score - b.score);
      return 1;
    }),
    zRange: jest.fn().mockImplementation(async (key: string, start: number, stop: number, options?: { BY?: string; REV?: boolean }) => {
      const set = mockSortedSets[key] || [];
      const values = [...set];
      if (options?.REV) {
        values.reverse();
      }
      const sliced = values.slice(start, stop + 1);
      return sliced.map(i => i.value);
    }),
    hIncrBy: jest.fn().mockImplementation(async (key: string, field: string, increment: number) => {
      if (!mockHashes[key]) {
        mockHashes[key] = {};
      }
      const val = parseInt(mockHashes[key][field] || '0') + increment;
      mockHashes[key][field] = val.toString();
      return val;
    }),
    hGetAll: jest.fn().mockImplementation(async (key: string) => {
      return mockHashes[key] || {};
    }),
    zCard: jest.fn().mockImplementation(async (key: string) => {
      return (mockSortedSets[key] || []).length;
    }),
    del: jest.fn().mockImplementation(async (key: string) => {
      delete mockStore[key];
      return 1;
    }),
    zRem: jest.fn().mockImplementation(async (key: string, value: string) => {
      if (mockSortedSets[key]) {
        mockSortedSets[key] = mockSortedSets[key].filter(i => i.value !== value);
      }
      return 1;
    }),
    lPush: jest.fn().mockImplementation(async (key: string, value: string) => {
      return 1;
    })
  };

  beforeEach(() => {
    mockStore = {};
    mockSortedSets = {};
    mockHashes = {};
    jest.clearAllMocks();
    (getRedisClient as jest.Mock).mockReturnValue(mockRedisClient);
  });

  describe('Dependency Cycle Detection', () => {
    it('should successfully create a task with no dependencies', async () => {
      const task = await TaskQueue.createTask('task1', 'testHandler', { foo: 'bar' });
      expect(task.name).toBe('task1');
      expect(task.dependencies).toEqual([]);
      expect(mockStore[`task:${task.id}`]).toBeDefined();
    });

    it('should successfully create a task with valid dependencies (no cycles)', async () => {
      const taskB = await TaskQueue.createTask('taskB', 'testHandler', {});
      const taskC = await TaskQueue.createTask('taskC', 'testHandler', {});

      const taskA = await TaskQueue.createTask('taskA', 'testHandler', {}, {
        dependencies: [taskB.id, taskC.id]
      });

      expect(taskA.name).toBe('taskA');
      expect(taskA.dependencies).toEqual([taskB.id, taskC.id]);
    });

    it('should reject task creation with direct self-dependency (A depends on A)', async () => {
      const tempId = 'self-dependent-id';
      const result = await TaskQueue.checkCircularDependencies(tempId, [tempId]);
      expect(result).toEqual([tempId, tempId]);
    });

    it('should reject task creation with a direct dependency cycle (A -> B -> A)', async () => {
      (uuidv4 as jest.Mock).mockReturnValueOnce('taskB-id');
      const taskB = await TaskQueue.createTask('taskB', 'testHandler', {}, {
        dependencies: ['taskA-id']
      });

      (uuidv4 as jest.Mock).mockReturnValueOnce('taskA-id');

      await expect(
        TaskQueue.createTask('taskA', 'testHandler', {}, {
          dependencies: [taskB.id]
        })
      ).rejects.toThrow(CircularDependencyError);
    });

    it('should reject task creation with a transitive dependency cycle (A -> B -> C -> A)', async () => {
      (uuidv4 as jest.Mock).mockReturnValueOnce('taskB-id');
      const taskB = await TaskQueue.createTask('taskB', 'testHandler', {}, {
        dependencies: ['taskC-id']
      });

      (uuidv4 as jest.Mock).mockReturnValueOnce('taskC-id');
      const taskC = await TaskQueue.createTask('taskC', 'testHandler', {}, {
        dependencies: ['taskA-id']
      });

      (uuidv4 as jest.Mock).mockReturnValueOnce('taskA-id');

      let thrownError: any = null;
      try {
        await TaskQueue.createTask('taskA', 'testHandler', {}, {
          dependencies: [taskB.id]
        });
      } catch (err) {
        thrownError = err;
      }

      expect(thrownError).toBeInstanceOf(CircularDependencyError);
      expect(thrownError.cycle).toEqual(['taskA-id', taskB.id, 'taskC-id', 'taskA-id']);
    });

    it('should successfully resolve a diamond dependency with no cycle (A -> B, A -> C, B -> D, C -> D)', async () => {
      const taskD = await TaskQueue.createTask('taskD', 'testHandler', {});
      
      const taskB = await TaskQueue.createTask('taskB', 'testHandler', {}, { dependencies: [taskD.id] });
      const taskC = await TaskQueue.createTask('taskC', 'testHandler', {}, { dependencies: [taskD.id] });

      const taskA = await TaskQueue.createTask('taskA', 'testHandler', {}, {
        dependencies: [taskB.id, taskC.id]
      });

      expect(taskA.name).toBe('taskA');
      expect(taskA.dependencies).toEqual([taskB.id, taskC.id]);
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

      mockStore['task:task-1'] = JSON.stringify(mockTask);

      await TaskQueue.retryTask('task-1');

      const savedTask = JSON.parse(mockStore['task:task-1']);

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

      mockStore['task:task-2'] = JSON.stringify(mockTask);

      await TaskQueue.retryTask('task-2');

      const savedTask = JSON.parse(mockStore['task:task-2']);
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

      mockStore['task:task-3'] = JSON.stringify(mockTask);

      const result = await TaskQueue.retryTask('task-3');

      expect(result).toBe(false);
      expect(mockStore['task:task-3']).toBeUndefined();
    });
  });

  describe('createTask (Fix #22)', () => {
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
});
