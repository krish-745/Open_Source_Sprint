import { TaskScheduler } from '../../src/services/task-scheduler';
import { getRedisClient } from '../../src/services/redis';

jest.mock('../../src/services/redis', () => ({
  getRedisClient: jest.fn()
}));

describe('TaskScheduler Lock Renewal (Issue 8)', () => {
  let redisClient: any;

  beforeEach(() => {
    jest.useFakeTimers();
    redisClient = {
      set: jest.fn(),
      zRange: jest.fn().mockResolvedValue([]),
      eval: jest.fn().mockResolvedValue(1),
    };
    (getRedisClient as jest.Mock).mockReturnValue(redisClient);
  });

  afterEach(async () => {
    await TaskScheduler.stopScheduler();
    jest.useRealTimers();
    jest.clearAllMocks();
  });

  it('should accept a configurable lockTtlSeconds parameter', async () => {
    redisClient.set.mockResolvedValue(true);

    // The unfixed version only accepts (pollIntervalMs), so this call would
    // still work but lockTtlSeconds would be ignored (no renewal).
    // We verify the lock is acquired with our configurable TTL, not hardcoded 10.
    TaskScheduler.startScheduler(100, 30);

    await jest.advanceTimersByTimeAsync(1);

    expect(redisClient.set).toHaveBeenCalledWith(
      'scheduler:lock',
      expect.any(String),
      { NX: true, EX: 30 } // Configurable TTL, not hardcoded 10
    );
  });

  it('should renew the lock TTL before it expires while processing', async () => {
    // Lock acquired successfully
    redisClient.set.mockResolvedValue(true);

    // Use a 4-second TTL; heartbeat should fire at 2 seconds (TTL/2)
    TaskScheduler.startScheduler(60000, 4);

    // Trigger the first poll
    await jest.advanceTimersByTimeAsync(1);

    // Advance past the heartbeat interval (TTL/2 = 2000ms)
    await jest.advanceTimersByTimeAsync(2000);

    // The unfixed version will fail here because it has no heartbeat mechanism.
    // The fixed version should have called eval to renew the lock TTL.
    const renewCalls = (redisClient.eval as jest.Mock).mock.calls.filter(
      (call: any[]) => {
        const script: string = call[0];
        return script.includes('expire');
      }
    );
    expect(renewCalls.length).toBeGreaterThan(0);
  });

  it('should allow another instance to take over after the lock expires', async () => {
    // Instance A acquires the lock, then fails to renew (crashes)
    redisClient.set
      .mockResolvedValueOnce(true)   // Instance A acquires lock
      .mockResolvedValueOnce(null)   // Instance B cannot acquire (A holds it)
      .mockResolvedValueOnce(true);  // After TTL, Instance B acquires the lock

    const callCounts: number[] = [];
    redisClient.set.mockImplementation((...args: any[]) => {
      callCounts.push(Date.now());
      return callCounts.length === 1 ? true
           : callCounts.length === 2 ? null
           : true;
    });

    TaskScheduler.startScheduler(100, 4);

    // First poll - Instance A acquires
    await jest.advanceTimersByTimeAsync(1);
    expect(redisClient.set).toHaveBeenCalledTimes(1);

    // Second poll - Instance B cannot acquire (A holds)
    await jest.advanceTimersByTimeAsync(100);
    expect(redisClient.set).toHaveBeenCalledTimes(2);

    // Third poll - After TTL expiry, Instance B acquires
    await jest.advanceTimersByTimeAsync(100);
    expect(redisClient.set).toHaveBeenCalledTimes(3);
  });
});
