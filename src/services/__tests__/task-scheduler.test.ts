import { TaskScheduler } from '../task-scheduler';
import { getRedisClient } from '../redis';

jest.mock('../redis', () => ({
  getRedisClient: jest.fn()
}));

describe('TaskScheduler', () => {
  let redisClient: any;

  beforeEach(() => {
    jest.useFakeTimers();
    redisClient = {
      set: jest.fn(),
      zRange: jest.fn().mockResolvedValue([]),
      get: jest.fn(),
      del: jest.fn(),
      eval: jest.fn().mockResolvedValue(1),
      zAdd: jest.fn(),
      zRem: jest.fn(),
    };
    (getRedisClient as jest.Mock).mockReturnValue(redisClient);
  });

  afterEach(async () => {
    await TaskScheduler.stopScheduler();
    jest.useRealTimers();
    jest.clearAllMocks();
  });

  describe('Concurrency & Lock Management', () => {
    it('should use an atomic Lua script (eval) to release lock to prevent race conditions', async () => {
      redisClient.set.mockResolvedValue(true);
      
      TaskScheduler.startScheduler(50);
      
      await jest.advanceTimersByTimeAsync(1);
      
      expect(redisClient.eval).toHaveBeenCalled();
      expect(redisClient.get).not.toHaveBeenCalledWith('scheduler:lock');
      expect(redisClient.del).not.toHaveBeenCalledWith('scheduler:lock');
    });

    it('should continue polling even if lock acquisition fails', async () => {
      redisClient.set
        .mockResolvedValueOnce(null)
        .mockResolvedValueOnce('OK');

      TaskScheduler.startScheduler(100);
      
      await jest.advanceTimersByTimeAsync(1);
      expect(redisClient.set).toHaveBeenCalledTimes(1);

      await jest.advanceTimersByTimeAsync(100);
      expect(redisClient.set).toHaveBeenCalledTimes(2);
    });

    it('should accept a configurable lockTtlSeconds parameter', async () => {
      redisClient.set.mockResolvedValue(true);

      TaskScheduler.startScheduler(100, 30);
      await jest.advanceTimersByTimeAsync(1);

      expect(redisClient.set).toHaveBeenCalledWith(
        'scheduler:lock',
        expect.any(String),
        { NX: true, EX: 30 }
      );
    });

    it('should renew the lock TTL before it expires while processing', async () => {
      redisClient.set.mockResolvedValue(true);
      
      // Simulate a long-running task batch by delaying zRange resolution
      let resolveZRange: any;
      const zRangePromise = new Promise((resolve) => {
        resolveZRange = resolve;
      });
      redisClient.zRange.mockReturnValueOnce(zRangePromise);

      TaskScheduler.startScheduler(60000, 4);
      
      // Fast-forward past the initial 1ms to trigger the first execution
      await jest.advanceTimersByTimeAsync(1);
      
      // Advance 2 seconds into the "long running batch" to trigger the heartbeat interval
      await jest.advanceTimersByTimeAsync(2000);

      const renewCalls = (redisClient.eval as jest.Mock).mock.calls.filter(
        (call: any[]) => {
          const script: string = call[0];
          return script.includes('expire');
        }
      );
      expect(renewCalls.length).toBeGreaterThan(0);
      
      // Clean up to let the promise resolve and finally block execute
      resolveZRange([]);
      await Promise.resolve();
    });

    it('should allow another instance to take over after the lock expires', async () => {
      redisClient.set
        .mockResolvedValueOnce(true)
        .mockResolvedValueOnce(null)
        .mockResolvedValueOnce(true);

      const callCounts: number[] = [];
      redisClient.set.mockImplementation((...args: any[]) => {
        callCounts.push(Date.now());
        return callCounts.length === 1 ? true
             : callCounts.length === 2 ? null
             : true;
      });

      TaskScheduler.startScheduler(100, 4);

      await jest.advanceTimersByTimeAsync(1);
      expect(redisClient.set).toHaveBeenCalledTimes(1);

      await jest.advanceTimersByTimeAsync(100);
      expect(redisClient.set).toHaveBeenCalledTimes(2);

      await jest.advanceTimersByTimeAsync(100);
      expect(redisClient.set).toHaveBeenCalledTimes(3);
    });
  });
});
