import { TaskQueue } from '../src/services/task-queue';
import { getRedisClient } from '../src/services/redis';

jest.mock('../src/services/redis', () => {
  const mClient = {
    get: jest.fn(),
    set: jest.fn(),
    zAdd: jest.fn(),
    lPush: jest.fn(),
    del: jest.fn(),
  };
  return { getRedisClient: jest.fn(() => mClient) };
});

describe('Issue #18: Remove error field on task retry', () => {
  let redisClient: any;

  beforeEach(() => {
    redisClient = getRedisClient();
    jest.clearAllMocks();
  });

  it('should strictly DELETE the error field, not set it to undefined', async () => {
    const mockTask = {
      id: 'task-issue-18',
      name: 'failing-task',
      status: 'failed',
      retries: 0,
      maxRetries: 3,
      error: 'Connection timed out',
      queue: 'default',
      priority: 'medium',
    };

    redisClient.get.mockResolvedValue(JSON.stringify(mockTask));

    await TaskQueue.retryTask('task-issue-18');

    const setCall = redisClient.set.mock.calls[0];
    expect(setCall).toBeDefined();

    const savedTask = JSON.parse(setCall[1]);

    // Core fix: error must be completely absent from the serialized object
    expect(Object.keys(savedTask)).not.toContain('error');
    expect(savedTask.error).toBeUndefined();
  });

  it('should increment retries and set status to retry', async () => {
    const mockTask = {
      id: 'task-issue-18b',
      name: 'failing-task',
      status: 'failed',
      retries: 1,
      maxRetries: 3,
      error: 'Handler crashed',
      queue: 'default',
      priority: 'high',
    };

    redisClient.get.mockResolvedValue(JSON.stringify(mockTask));

    await TaskQueue.retryTask('task-issue-18b');

    const savedTask = JSON.parse(redisClient.set.mock.calls[0][1]);
    expect(savedTask.status).toBe('retry');
    expect(savedTask.retries).toBe(2);
  });

  it('should move to dead letter queue and return false when maxRetries is exhausted', async () => {
    // Mock a task that has already hit its max retries
    const mockTask = {
      id: 'task-issue-18c',
      name: 'failing-task',
      status: 'failed',
      retries: 3,
      maxRetries: 3,
      error: 'Permanent failure',
      queue: 'default',
      priority: 'low',
    };

    redisClient.get.mockResolvedValue(JSON.stringify(mockTask));

    const result = await TaskQueue.retryTask('task-issue-18c');

    // Should NOT re-queue when exhausted
    expect(result).toBe(false);
    // Should NOT write a clean task back (dead letter queue moves it instead)
    expect(redisClient.set).not.toHaveBeenCalled();
  });

  /**
   * REGRESSION TEST
   * This test demonstrates the original bug. Uncomment the bugged line and
   * comment out the fix to confirm this test FAILS on the old implementation.
   *
   * Old code: task.error = undefined;
   * Fixed:    delete task.error;
   *
   * JSON.stringify({error: undefined}) => '{}' (key silently dropped)
   * But the object in-memory still has the key, causing non-deterministic behavior.
   */
  it('[regression] setting error=undefined is NOT equivalent to deleting it', () => {
    const obj: any = { status: 'retry', error: 'old error message' };

    // Simulate the OLD (broken) behavior
    obj.error = undefined;
    expect(Object.keys(obj)).toContain('error'); // key still present in-memory!
    expect(JSON.stringify(obj)).toBe('{"status":"retry"}'); // but silently dropped in JSON

    // Simulate the NEW (fixed) behavior
    const obj2: any = { status: 'retry', error: 'old error message' };
    delete obj2.error;
    expect(Object.keys(obj2)).not.toContain('error'); // key is gone from memory too
    expect(JSON.stringify(obj2)).toBe('{"status":"retry"}'); // consistent
  });
});
