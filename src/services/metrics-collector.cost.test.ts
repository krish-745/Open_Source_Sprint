import { MetricsCollector } from './metrics-collector';
import { WorkerPool } from './worker-pool';
import { getRedisClient } from './redis';

jest.mock('./worker-pool', () => ({
  WorkerPool: {
    getWorkerMetrics: jest.fn(),
  }
}));

jest.mock('./redis', () => {
  const store: Record<string, any> = {};
  return {
    getRedisClient: jest.fn(() => ({
      keys: jest.fn(async () => []),
      zRange: jest.fn(async () => ['w1', 'w2']),
      zCard: jest.fn(async () => 0),
      lLen: jest.fn(async () => 0),
      set: jest.fn(async (k, v) => { store[k] = v; return 'OK'; }),
      get: jest.fn(async (k) => store[k]),
      del: jest.fn(),
    })),
  };
});

describe('MetricsCollector Cost Aggregation', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should aggregate totalCostIncurred from all workers', async () => {
    const mockWorkerMetrics = jest.spyOn(WorkerPool, 'getWorkerMetrics')
      .mockResolvedValueOnce({
        workerId: 'w1',
        name: 'w1',
        status: 'online',
        currentTasks: 0,
        totalProcessed: 1,
        totalFailed: 0,
        successRate: '100.00',
        capacity: 0,
        handlers: [],
        lastHeartbeat: new Date(),
        totalCostIncurred: 150
      } as any)
      .mockResolvedValueOnce({
        workerId: 'w2',
        name: 'w2',
        status: 'online',
        currentTasks: 0,
        totalProcessed: 1,
        totalFailed: 0,
        successRate: '100.00',
        capacity: 0,
        handlers: [],
        lastHeartbeat: new Date(),
        totalCostIncurred: 250
      } as any);

    const snapshot = await MetricsCollector.captureSnapshot();
    
    expect(snapshot.totalCostIncurred).toBe(400);
    expect(snapshot.workers['w1'].totalCostIncurred).toBe(150);
    expect(snapshot.workers['w2'].totalCostIncurred).toBe(250);
  });
});
