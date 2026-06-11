import { WorkerPool } from '../worker-pool';
import { TaskQueue } from '../task-queue';
import { getRedisClient } from '../redis';
import { Worker, Task } from '../../types';

// Mock the Redis client using an in-memory store
const store: Record<string, string> = {};
const listStore: Record<string, string[]> = {};
let workersIndex: string[] = [];

const mockRedisClient = {
  get: jest.fn().mockImplementation(async (key: string) => store[key] || null),
  set: jest.fn().mockImplementation(async (key: string, value: string) => {
    store[key] = value;
    return 'OK';
  }),
  zRange: jest.fn().mockImplementation(async (key: string, start: number, stop: number) => {
    return workersIndex;
  }),
  lRange: jest.fn().mockImplementation(async (key: string, start: number, stop: number) => {
    return listStore[key] || [];
  }),
  lPush: jest.fn().mockImplementation(async (key: string, value: string) => {
    if (!listStore[key]) listStore[key] = [];
    listStore[key].push(value);
    return listStore[key].length;
  }),
  del: jest.fn().mockImplementation(async (key: string) => {
    delete store[key];
    delete listStore[key];
    return 1;
  }),
  zAdd: jest.fn().mockResolvedValue(1),
};

jest.mock('../redis', () => ({
  getRedisClient: () => mockRedisClient,
}));

// Mock TaskQueue methods
jest.mock('../task-queue', () => {
  const original = jest.requireActual('../task-queue');
  return {
    TaskQueue: {
      getTask: jest.fn(),
      retryTask: jest.fn(),
      updateTaskStatus: jest.fn(),
      reclaimStuckTasks: jest.fn().mockImplementation(async (workerId: string) => {
        // Simple mock implementation of reclaimStuckTasks to verify it's called
        const client = getRedisClient();
        const taskIds = await client.lRange(`worker:${workerId}:tasks`, 0, -1);
        for (const taskId of taskIds) {
          const task = await TaskQueue.getTask(taskId);
          if (task && task.status === 'processing') {
            task.workerId = undefined;
            await client.set(`task:${taskId}`, JSON.stringify(task));
            await TaskQueue.retryTask(taskId);
          }
        }
        await client.del(`worker:${workerId}:tasks`);
        return taskIds.length;
      }),
    },
  };
});

describe('WorkerPool - checkStaleWorkers and Task Reclamation', () => {
  beforeEach(() => {
    for (const key in store) delete store[key];
    for (const key in listStore) delete listStore[key];
    workersIndex = [];
    jest.clearAllMocks();
  });

  const createWorkerMock = (id: string, status: any, lastHeartbeat: Date): Worker => {
    return {
      id,
      name: `Worker ${id}`,
      status,
      handlers: ['testHandler'],
      maxConcurrent: 5,
      currentTasks: 1,
      totalProcessed: 10,
      totalFailed: 0,
      lastHeartbeat,
      registeredAt: new Date(),
      version: '1.0.0',
      capacity: 20,
      tags: [],
    };
  };

  const createTaskMock = (id: string, status: any, workerId?: string): Task => {
    return {
      id,
      name: 'Stuck Task',
      description: 'Desc',
      priority: 'medium',
      status,
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
      workerId,
    };
  };

  it('should mark stale workers as offline and reclaim their processing tasks', async () => {
    const workerId = 'stale-worker-1';
    const taskId = 'stuck-task-1';
    
    // Heartbeat from 5 minutes ago (timeout is 60s)
    const lastHeartbeat = new Date(Date.now() - 5 * 60 * 1000);
    const worker = createWorkerMock(workerId, 'online', lastHeartbeat);
    const task = createTaskMock(taskId, 'processing', workerId);

    // Save mocks in database
    store[`worker:${workerId}`] = JSON.stringify(worker);
    store[`task:${taskId}`] = JSON.stringify(task);
    workersIndex = [workerId];
    listStore[`worker:${workerId}:tasks`] = [taskId];

    // Mock TaskQueue method returns
    (TaskQueue.getTask as jest.Mock).mockResolvedValue(task);
    (TaskQueue.retryTask as jest.Mock).mockResolvedValue(true);

    const staleCount = await WorkerPool.checkStaleWorkers(60);
    expect(staleCount).toBe(1);

    // Verify worker status is set to offline and capacity reset
    const updatedWorkerData = store[`worker:${workerId}`];
    expect(updatedWorkerData).toBeDefined();
    const updatedWorker: Worker = JSON.parse(updatedWorkerData);
    expect(updatedWorker.status).toBe('offline');
    expect(updatedWorker.currentTasks).toBe(0);
    expect(updatedWorker.capacity).toBe(0);

    // Verify task queue reclaimStuckTasks was called
    expect(TaskQueue.reclaimStuckTasks).toHaveBeenCalledWith(workerId);
  });
});
