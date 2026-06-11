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

describe('TaskQueue - Dependency Cycle Detection', () => {
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
    })
  };

  beforeEach(() => {
    mockStore = {};
    mockSortedSets = {};
    mockHashes = {};
    jest.clearAllMocks();
    (getRedisClient as jest.Mock).mockReturnValue(mockRedisClient);
  });

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
