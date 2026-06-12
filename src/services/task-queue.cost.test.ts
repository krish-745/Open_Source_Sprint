import { TaskQueue } from './task-queue';
import { getRedisClient } from './redis';

// Mock Redis client
jest.mock('./redis', () => {
  const store: Record<string, any> = {};
  const queueStore: Record<string, string[]> = {};

  return {
    getRedisClient: jest.fn(() => ({
      get: jest.fn(async (key: string) => store[key] || null),
      set: jest.fn(async (key: string, value: string) => {
        store[key] = value;
        return 'OK';
      }),
      mGet: jest.fn(async (keys: string[]) => keys.map((k) => store[k] || null)),
      zAdd: jest.fn(async (key: string, item: any) => {
        if (!queueStore[key]) queueStore[key] = [];
        queueStore[key] = queueStore[key].filter(v => v !== item.value);
        queueStore[key].push(item.value);
        return 1;
      }),
      zRem: jest.fn(async (key: string, value: string) => {
        if (queueStore[key]) {
          queueStore[key] = queueStore[key].filter((v) => v !== value);
        }
        return 1;
      }),
      zRange: jest.fn(async (key: string) => {
        return queueStore[key] || [];
      }),
      zCard: jest.fn(async (key: string) => queueStore[key]?.length || 0),
      hGetAll: jest.fn(async () => ({})),
      hIncrBy: jest.fn(async () => 1),
      del: jest.fn(async (key: string) => {
        delete store[key];
        return 1;
      }),
      multi: jest.fn().mockImplementation(function() {
        const m: any = {
          set: jest.fn().mockReturnThis(),
          zRem: jest.fn().mockReturnThis(),
          zAdd: jest.fn().mockReturnThis(),
          exec: jest.fn().mockResolvedValue([['OK']]),
        };
        return m;
      }),
    })),
  };
});

describe('TaskQueue Cost Estimation', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should estimate cost based on budget and costEstimate', async () => {
    // Create tasks with different cost values
    await TaskQueue.createTask('task1', 'handler', {}, { 
      queueName: 'costQueue',
      budget: 10 
    });
    
    await TaskQueue.createTask('task2', 'handler', {}, { 
      queueName: 'costQueue',
      costEstimate: { money: 20 }
    });
    
    await TaskQueue.createTask('task3', 'handler', {}, { 
      queueName: 'costQueue' // Fallback to 1
    });

    const cost = await TaskQueue.estimateQueueCost('costQueue');
    // task1 (10) + task2 (20) + task3 (1) = 31
    expect(cost).toBe(31);
  });
});
