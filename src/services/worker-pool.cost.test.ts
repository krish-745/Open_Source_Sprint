import { WorkerPool } from './worker-pool';
import { getRedisClient } from './redis';
import { Task } from '../types';

// Mock Redis client
jest.mock('./redis', () => {
  const store: Record<string, any> = {};
  const sets: Record<string, string[]> = {};

  return {
    getRedisClient: jest.fn(() => ({
      get: jest.fn(async (key: string) => store[key] || null),
      set: jest.fn(async (key: string, value: string) => {
        store[key] = value;
        return 'OK';
      }),
      sAdd: jest.fn(async (key: string, value: string) => {
        if (!sets[key]) sets[key] = [];
        if (!sets[key].includes(value)) sets[key].push(value);
        return 1;
      }),
      sMembers: jest.fn(async (key: string) => {
        return sets[key] || [];
      }),
      zAdd: jest.fn(async () => 1),
    })),
  };
});

describe('WorkerPool Cost-based Selection', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should filter out workers that exceed the budget and sort by cost', async () => {
    // Register expensive worker
    await WorkerPool.registerWorker('expensive', ['costHandler'], {
      baseCostPerTask: 50,
      resourceRatings: { cpu: 10, memory: 5 }
    });

    // Register cheap worker
    await WorkerPool.registerWorker('cheap', ['costHandler'], {
      baseCostPerTask: 10,
      resourceRatings: { cpu: 2, memory: 1 }
    });

    // Register out of budget worker
    await WorkerPool.registerWorker('luxury', ['costHandler'], {
      baseCostPerTask: 200,
    });

    const task: Task = {
      id: 't1',
      name: 't1',
      description: '',
      priority: 'medium',
      status: 'pending',
      handler: 'costHandler',
      payload: {},
      retries: 0,
      maxRetries: 3,
      timeout: 1000,
      createdAt: new Date(),
      queue: 'default',
      dependencies: [],
      tags: [],
      metadata: {},
      budget: 100, // Luxury is 200, so it should be excluded
      costEstimate: { cpu: 2, memory: 1 }
    };

    const available = await WorkerPool.getAvailableWorkers('costHandler', task);
    
    expect(available).toHaveLength(2);
    // cheap cost: 10 + (2*2) + (1*1) = 15
    // expensive cost: 50 + (10*2) + (5*1) = 75
    // luxury cost: 200 (excluded)
    
    expect(available[0].name).toBe('cheap');
    expect(available[1].name).toBe('expensive');
  });
});
