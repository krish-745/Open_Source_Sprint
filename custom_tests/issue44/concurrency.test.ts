import { TaskScheduler } from '../../src/services/task-scheduler';
import { getRedisClient } from '../../src/services/redis';

// Mock the Redis client
jest.mock('../../src/services/redis', () => ({
  getRedisClient: jest.fn()
}));

describe('TaskScheduler Concurrency & Polling (Issue 44)', () => {
  let redisClient: any;

  beforeEach(() => {
    jest.useFakeTimers();
    redisClient = {
      set: jest.fn(),
      zRange: jest.fn().mockResolvedValue([]),
      get: jest.fn(),
      del: jest.fn(),
      eval: jest.fn(),
    };
    (getRedisClient as jest.Mock).mockReturnValue(redisClient);
  });

  afterEach(async () => {
    await TaskScheduler.stopScheduler();
    jest.useRealTimers();
    jest.clearAllMocks();
  });

  it('should continue polling even if lock acquisition fails', async () => {
    // 1st poll: lock acquisition fails (simulate another instance holds the lock)
    // 2nd poll: lock acquisition succeeds
    redisClient.set
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce('OK');

    // Start the scheduler with a 100ms interval
    TaskScheduler.startScheduler(100);
    
    // Fast-forward to execute the first poll
    jest.advanceTimersByTime(1);
    
    // Await all microtasks
    await Promise.resolve();
    
    // First poll should have tried to acquire the lock and failed
    expect(redisClient.set).toHaveBeenCalledTimes(1);

    // Fast-forward past the polling interval to trigger the next poll
    jest.advanceTimersByTime(100);
    await Promise.resolve();
    await Promise.resolve(); // Flush microtasks just in case
    
    // The unfixed scheduler will fail here because it returns early when lock fails
    // and never schedules the next setTimeout.
    expect(redisClient.set).toHaveBeenCalledTimes(2);
  });
});
