import { TaskQueue } from './task-queue';
import * as redis from './redis';
import { Task } from '../types';
import { TaskExecutor } from './task-executor';
import { WorkerPool } from './worker-pool';

jest.mock('./redis');
jest.mock('./worker-pool', () => ({
  WorkerPool: {
    updateWorkerStatus: jest.fn(),
    completeTask: jest.fn(),
    getWorker: jest.fn(),
  }
}));

const mockedGetRedisClient = redis.getRedisClient as jest.Mock;

function buildTask(overrides: Partial<Task> = {}): Task {
  return {
    id: 'task',
    name: 'demo',
    description: 'Task: demo',
    priority: 'medium',
    status: 'queued',
    handler: 'noop',
    payload: {},
    retries: 0,
    maxRetries: 3,
    timeout: 30000,
    createdAt: new Date(),
    queue: 'default',
    dependencies: [],
    tags: [],
    metadata: {},
    ttl: 3600, // 1 hour
    ...overrides,
  };
}

afterEach(() => jest.clearAllMocks());

describe('Task Time-To-Live (TTL)', () => {
  describe('TaskQueue.getNextTask', () => {
    it('skips and cancels an expired task', async () => {
      const now = Date.now();
      const pastTime = new Date(now - 7200000); // 2 hours ago

      const store: Record<string, string> = {
        'task:expired': JSON.stringify(buildTask({ 
          id: 'expired', 
          createdAt: pastTime, 
          ttl: 3600, // 1 hour TTL
          status: 'pending' 
        })),
        'task:valid': JSON.stringify(buildTask({ 
          id: 'valid', 
          createdAt: new Date(), 
          ttl: 3600, 
          status: 'pending' 
        })),
      };

      const client: any = {
        zRange: jest.fn().mockResolvedValue(['expired', 'valid']),
        get: jest.fn((k: string) => Promise.resolve(store[k] ?? null)),
        set: jest.fn().mockResolvedValue('OK'),
        zRem: jest.fn().mockResolvedValue(1),
        incr: jest.fn().mockResolvedValue(1),
        hIncrBy: jest.fn().mockResolvedValue(1),
      };
      mockedGetRedisClient.mockReturnValue(client);

      const next = await TaskQueue.getNextTask('default');
      
      // Should skip 'expired' and return 'valid'
      expect(next?.id).toBe('valid');
      
      // Should have cancelled 'expired'
      expect(client.set).toHaveBeenCalled();
      const setCall = client.set.mock.calls.find((call: any[]) => call[0] === 'task:expired');
      expect(setCall).toBeDefined();
      const updatedTask = JSON.parse(setCall[1]);
      expect(updatedTask.status).toBe('cancelled');
      
      // Should remove from queue and increment metrics
      expect(client.zRem).toHaveBeenCalledWith('queue:default', 'expired');
      expect(client.incr).toHaveBeenCalledWith('metrics:tasks:expired');
    });
  });

  describe('TaskExecutor.execute', () => {
    it('cancels the task if it expired before execution starts', async () => {
      const now = Date.now();
      const pastTime = new Date(now - 7200000); // 2 hours ago

      const expiredTask = buildTask({ 
        id: 'expired_exec', 
        createdAt: pastTime, 
        ttl: 3600,
        status: 'queued' 
      });

      const client: any = {
        set: jest.fn().mockResolvedValue('OK'),
        get: jest.fn().mockResolvedValue(JSON.stringify(expiredTask)),
      };
      mockedGetRedisClient.mockReturnValue(client);

      TaskExecutor.registerHandler('noop', async () => {});

      await TaskExecutor.execute('worker-1', expiredTask);

      // Verify that the task status was updated to cancelled
      expect(client.set).toHaveBeenCalled();
      const setCall = client.set.mock.calls.find((call: any[]) => call[0] === 'task:expired_exec');
      expect(setCall).toBeDefined();
      const updatedTask = JSON.parse(setCall[1]);
      expect(updatedTask.status).toBe('cancelled');

      // WorkerPool.completeTask should have been called
      expect(WorkerPool.completeTask).toHaveBeenCalledWith('worker-1', 'expired_exec', expect.objectContaining({
        success: false
      }));
    });
  });
});
