import { TaskQueue } from '../../src/services/task-queue';
import { WorkerPool } from '../../src/services/worker-pool';
import { MetricsCollector } from '../../src/services/metrics-collector';
import { getRedisClient } from '../../src/services/redis';

const store = new Map();
const mockClient = {
  set: jest.fn(async (key: string, value: string) => store.set(key, value)),
  get: jest.fn(async (key: string) => store.get(key)),
  zAdd: jest.fn(),
  hIncrBy: jest.fn(async (key: string, field: string, incr: number) => {
    if (!store.has(key)) store.set(key, new Map());
    const hash = store.get(key);
    const curr = parseInt(hash.get(field) || '0', 10);
    const next = curr + incr;
    hash.set(field, next.toString());
    return next;
  }),
  hSet: jest.fn(async (key: string, field: string, val: string) => {
    if (!store.has(key)) store.set(key, new Map());
    store.get(key).set(field, val);
  }),
  hGet: jest.fn(async (key: string, field: string) => {
    if (!store.has(key)) return null;
    return store.get(key).get(field) || null;
  }),
  zCard: jest.fn(async () => 0),
  hGetAll: jest.fn(),
  zRange: jest.fn(),
  sAdd: jest.fn(),
  sMembers: jest.fn(async () => [] as string[]),
  sRem: jest.fn(),
  lPush: jest.fn(),
  lRem: jest.fn(),
  del: jest.fn(),
  zRem: jest.fn(),
  keys: jest.fn(async () => [] as string[]),
};

jest.mock('../../src/services/redis', () => {
  return {
    getRedisClient: jest.fn(() => mockClient),
  };
});

describe('Issue #42: Cost-Based Scheduling', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    store.clear();
  });

  it('should reject task creation if caller budget is exceeded', async () => {
    if (typeof (TaskQueue as any).setCallerBudget !== 'function') {
      throw new Error('TaskQueue.setCallerBudget is not implemented');
    }
    await (TaskQueue as any).setCallerBudget('alice', 40);

    let caughtError;
    try {
      await TaskQueue.createTask('expensive_task', 'handler', {}, {
        metadata: { cost: 50, callerId: 'alice' }
      });
    } catch (err: any) {
      caughtError = err;
    }
    expect(caughtError).toBeDefined();
    expect(caughtError.message).toMatch(/budget exceeded/i);
  });

  it('should return cheapest workers first', async () => {
    const w1 = await WorkerPool.registerWorker('expensive-worker', ['my-handler'], { maxConcurrent: 5 });
    const w2 = await WorkerPool.registerWorker('cheap-worker', ['my-handler'], { maxConcurrent: 5 });
    
    if (typeof (WorkerPool as any).setWorkerCost !== 'function') {
      throw new Error('WorkerPool.setWorkerCost is not implemented');
    }

    await (WorkerPool as any).setWorkerCost(w1.id, 20);
    await (WorkerPool as any).setWorkerCost(w2.id, 5);

    mockClient.sMembers.mockResolvedValueOnce([w1.id, w2.id]);
    
    const workers = await WorkerPool.getAvailableWorkers('my-handler');
    expect(workers.length).toBe(2);
    expect(workers[0].id).toBe(w2.id); // w2 is cheaper (5 vs 20)
    expect(workers[1].id).toBe(w1.id);
  });

  it('should predict queue cost by summing up tasks cost', async () => {
    const t1 = await TaskQueue.createTask('t1', 'h1', {}, { queueName: 'predict-q', metadata: { cost: 10 } });
    const t2 = await TaskQueue.createTask('t2', 'h1', {}, { queueName: 'predict-q', metadata: { cost: 15 } });
    
    mockClient.zRange.mockResolvedValueOnce([t1.id, t2.id]);

    if (typeof (TaskQueue as any).predictQueueCost !== 'function') {
      throw new Error('TaskQueue.predictQueueCost is not implemented');
    }
    const result = await (TaskQueue as any).predictQueueCost('predict-q');
    expect(result.totalCost).toBe(25);
    expect(result.taskCount).toBe(2);
  });

  it('should accumulate total cost on worker when tasks complete', async () => {
    const w1 = await WorkerPool.registerWorker('costly-worker', ['h2']);
    
    if (typeof (WorkerPool as any).getWorkerCostMetrics !== 'function') {
      throw new Error('WorkerPool.getWorkerCostMetrics is not implemented');
    }
    
    // A task gets assigned to the worker
    const task = await TaskQueue.createTask('t1', 'h2', {}, { metadata: { cost: 12 } });
    await WorkerPool.assignTask(w1.id, task);

    // Complete the task, passing the task object in metrics if the implementation uses it to know the cost.
    // Let's pass the taskId in the metrics so the worker pool can fetch the task to find its cost!
    // But completeTask signature: `completeTask(workerId: string, taskId: string, metrics: Partial<TaskExecutionMetrics>)`
    await WorkerPool.completeTask(w1.id, task.id, { success: true });
    
    const metrics = await (WorkerPool as any).getWorkerCostMetrics(w1.id);
    expect(metrics.totalCostAccrued).toBeGreaterThan(0);
    expect(metrics.totalCostAccrued).toBe(12);
  });
});
