import { TaskScheduler } from '../../src/services/task-scheduler';
import { getRedisClient } from '../../src/services/redis';

// Mock the Redis client
jest.mock('../../src/services/redis', () => ({
  getRedisClient: jest.fn()
}));

describe('TaskScheduler Distributed Lock (Issue 101)', () => {
  let redisClient: any;

  beforeEach(() => {
    redisClient = {
      set: jest.fn(),
      zRange: jest.fn().mockResolvedValue([]),
      get: jest.fn(),
      del: jest.fn(),
      eval: jest.fn().mockResolvedValue(1), // Mock Lua script evaluation
    };
    (getRedisClient as jest.Mock).mockReturnValue(redisClient);
  });

  afterEach(async () => {
    await TaskScheduler.stopScheduler();
    jest.clearAllMocks();
  });

  it('should use atomic Lua script (eval) to release lock to prevent race conditions', async () => {
    // Simulate successfully acquiring the lock
    redisClient.set.mockResolvedValue(true);
    
    // Start the scheduler with a very short poll interval
    await TaskScheduler.startScheduler(50);
    
    // Wait long enough for the first polling cycle to complete its execution
    await new Promise(resolve => setTimeout(resolve, 20));
    
    // Verify that the scheduler is using the atomic `eval` command to release the lock,
    // rather than the vulnerable `get` followed by `del` sequence.
    expect(redisClient.eval).toHaveBeenCalled();
    
    // The unfixed version will fail here because it calls get() and del()
    expect(redisClient.get).not.toHaveBeenCalledWith('scheduler:lock');
    expect(redisClient.del).not.toHaveBeenCalledWith('scheduler:lock');
  });
});
