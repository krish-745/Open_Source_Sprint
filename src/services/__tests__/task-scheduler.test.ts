import { TaskScheduler } from '../task-scheduler';
import { getRedisClient } from '../redis';
import { TaskQueue } from '../task-queue';

jest.mock('../redis', () => {
  const mClient = {
    get: jest.fn(),
    set: jest.fn(),
    del: jest.fn(),
    expire: jest.fn(),
    zAdd: jest.fn(),
    zRange: jest.fn(),
    zRem: jest.fn(),
    zRangeWithScores: jest.fn(),
  };
  return { getRedisClient: jest.fn(() => mClient) };
});

jest.mock('../task-queue', () => {
  return {
    TaskQueue: {
      getTask: jest.fn(),
      updateTaskStatus: jest.fn(),
      recoverStaleTasks: jest.fn(),
    },
  };
});

const flushPromises = async () => {
  for (let i = 0; i < 20; i++) {
    await Promise.resolve();
  }
};

describe('TaskScheduler', () => {
  beforeEach(() => {
    jest.useFakeTimers();
    jest.clearAllMocks();
  });

  afterEach(async () => {
    await TaskScheduler.stopScheduler();
    jest.useRealTimers();
  });

  describe('Distributed Lock and Takeover', () => {
    let lockStore: Record<string, string> = {};

    beforeEach(() => {
      lockStore = {};
      const client = getRedisClient() as any;

      client.set.mockImplementation(async (key: string, value: string, options: any) => {
        if (options && options.NX) {
          if (lockStore[key]) {
            return null; // already exists, set NX fails
          }
          lockStore[key] = value;
          return 'OK';
        }
        lockStore[key] = value;
        return 'OK';
      });

      client.get.mockImplementation(async (key: string) => {
        return lockStore[key] || null;
      });

      client.del.mockImplementation(async (key: string) => {
        delete lockStore[key];
        return 1;
      });

      client.expire.mockResolvedValue(1);
      client.zRange.mockResolvedValue([]);
      client.zRem.mockResolvedValue(1);
    });

    it('should acquire lock, process due tasks, and release lock', async () => {
      const client = getRedisClient() as any;
      client.zRange.mockResolvedValue([JSON.stringify({ taskId: 't-sched-1' })]);

      const mockTask = { id: 't-sched-1', status: 'pending', queue: 'default' };
      (TaskQueue.getTask as jest.Mock).mockResolvedValue(mockTask);

      await TaskScheduler.startScheduler(5000);
      await flushPromises();

      expect(client.set).toHaveBeenCalledWith('scheduler:lock', expect.stringMatching(/^scheduler-/), {
        NX: true,
        EX: TaskScheduler.lockTtlSeconds,
      });
      expect(TaskQueue.updateTaskStatus).toHaveBeenCalledWith('t-sched-1', 'queued');
      expect(client.zRem).toHaveBeenCalledWith('scheduled:tasks', expect.any(String));
      expect(TaskQueue.recoverStaleTasks).toHaveBeenCalled();
      expect(client.del).toHaveBeenCalledWith('scheduler:lock');
    });

    it('should renew lock before expiration when processing is active', async () => {
      const client = getRedisClient() as any;

      // Mock recoverStaleTasks to be slow
      let resolveStale: any;
      const stalePromise = new Promise((resolve) => {
        resolveStale = resolve;
      });
      (TaskQueue.recoverStaleTasks as jest.Mock).mockReturnValue(stalePromise);

      TaskScheduler.lockTtlSeconds = 10;
      TaskScheduler.renewalIntervalMs = 1000;

      await TaskScheduler.startScheduler(5000);
      await flushPromises();

      expect(client.expire).not.toHaveBeenCalled();

      // Advance timers by 1000ms to trigger the first renewal interval
      jest.advanceTimersByTime(1000);
      await flushPromises();

      expect(client.expire).toHaveBeenCalledWith('scheduler:lock', 10);

      // Resolve the slow promise to finish execution and release lock
      resolveStale();
      await flushPromises();

      expect(client.del).toHaveBeenCalledWith('scheduler:lock');
    });

    it('should not process due tasks if lock is not acquired', async () => {
      const client = getRedisClient() as any;
      
      // Pre-acquire the lock by another owner
      lockStore['scheduler:lock'] = 'other-scheduler-instance';

      await TaskScheduler.startScheduler(5000);
      await flushPromises();

      expect(client.zRange).not.toHaveBeenCalled();
      // del should not be called since we didn't own the lock
      expect(client.del).not.toHaveBeenCalled();
    });
  });

  describe('Delayed and Recurring Scheduling API', () => {
    it('should schedule a delayed task', async () => {
      const client = getRedisClient() as any;
      client.zAdd.mockResolvedValue(1);

      const callback = async () => {};
      await TaskScheduler.scheduleDelayed('t-delay-1', 1000, callback);

      expect(client.zAdd).toHaveBeenCalledWith('scheduled:tasks', expect.objectContaining({
        score: expect.any(Number),
        value: expect.stringContaining('t-delay-1'),
      }));
    });

    it('should return pending scheduled tasks', async () => {
      const client = getRedisClient() as any;
      client.zRangeWithScores.mockResolvedValue([
        { value: JSON.stringify({ taskId: 't-pending-1' }), score: 12345 }
      ]);

      const pending = await TaskScheduler.getPendingScheduledTasks();
      expect(pending).toHaveLength(1);
      expect(pending[0].taskId).toBe('t-pending-1');
      expect(pending[0].scheduledAt).toBe(12345);
    });
  });
});
