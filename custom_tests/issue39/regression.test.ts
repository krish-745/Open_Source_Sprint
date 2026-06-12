/**
 * Regression test for Issue #39: Implement Distributed Tracing
 *
 * Each test is designed to FAIL on the unmodified codebase and PASS after the fix.
 */
import { TaskQueue } from '../../src/services/task-queue';
import { getRedisClient } from '../../src/services/redis';
import { TaskExecutor } from '../../src/services/task-executor';
import { withTrace } from '../../src/utils/logger';

jest.mock('../../src/services/redis', () => {
  const mClient = {
    get: jest.fn(),
    set: jest.fn(),
    zAdd: jest.fn(),
    zCard: jest.fn(),
    hIncrBy: jest.fn(),
    lPush: jest.fn(),
    del: jest.fn(),
    sAdd: jest.fn(),
    sMembers: jest.fn(),
    expire: jest.fn(),
  };
  return { getRedisClient: jest.fn(() => mClient) };
});

jest.mock('../../src/utils/logger', () => {
  const childLogger = {
    info: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
  };
  const rootLogger = {
    info: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
    child: jest.fn(() => childLogger),
  };
  return {
    __esModule: true,
    default: rootLogger,
    withTrace: jest.fn(() => childLogger),
    _childLogger: childLogger,
  };
});

describe('Issue #39: Distributed Tracing', () => {
  let redisClient: any;

  beforeEach(() => {
    redisClient = getRedisClient();
    jest.clearAllMocks();
  });

  // ── Task.traceId field ────────────────────────────────────────────────────

  it('Task type has traceId and parentSpanId fields', async () => {
    redisClient.zCard.mockResolvedValue(0);
    const task = await TaskQueue.createTask('root-task', 'handler', {});
    // Property must exist on the returned object (not just in the type)
    expect(Object.prototype.hasOwnProperty.call(task, 'traceId')).toBe(true);
    expect(task.traceId).toBeDefined();
    expect(typeof task.traceId).toBe('string');
    // Root task has no parent span
    expect(task.parentSpanId).toBeUndefined();
  });

  // ── Trace propagation through dependency chain ────────────────────────────

  it('inherits traceId and sets parentSpanId from the first dependency', async () => {
    const depTask = {
      id: 'dep-1',
      status: 'completed',
      traceId: 'shared-trace-id',
    };
    redisClient.get.mockResolvedValue(JSON.stringify(depTask));
    redisClient.zCard.mockResolvedValue(0);

    const childTask = await TaskQueue.createTask('child-task', 'handler', {}, {
      dependencies: ['dep-1'],
    });

    expect(childTask.traceId).toBe('shared-trace-id');
    // parentSpanId must point at the DIRECT dependency (the parent span)
    expect(childTask.parentSpanId).toBe('dep-1');
  });

  it('respects an explicitly supplied traceId and parentSpanId over auto-derived values', async () => {
    const depTask = { id: 'dep-x', status: 'completed', traceId: 'auto-trace' };
    redisClient.get.mockResolvedValue(JSON.stringify(depTask));
    redisClient.zCard.mockResolvedValue(0);

    const task = await TaskQueue.createTask('override-task', 'handler', {}, {
      dependencies: ['dep-x'],
      traceId: 'explicit-trace',
      parentSpanId: 'explicit-parent',
    });

    expect(task.traceId).toBe('explicit-trace');
    expect(task.parentSpanId).toBe('explicit-parent');
  });

  // ── Trace index (Redis set) ───────────────────────────────────────────────

  it('adds the task to the Redis trace index and sets a TTL', async () => {
    redisClient.zCard.mockResolvedValue(0);
    const task = await TaskQueue.createTask('ttl-task', 'handler', {});

    const traceKey = `trace:${task.traceId}`;
    expect(redisClient.sAdd).toHaveBeenCalledWith(traceKey, task.id);
    // TTL must be set — without it the sets accumulate forever
    expect(redisClient.expire).toHaveBeenCalledWith(traceKey, expect.any(Number));
    const ttl: number = redisClient.expire.mock.calls[0][1];
    expect(ttl).toBeGreaterThan(0);
  });

  // ── Query by traceId ──────────────────────────────────────────────────────

  it('getTasksByTraceId returns all tasks in the trace', async () => {
    redisClient.sMembers.mockResolvedValue(['t1', 't2', 't3']);
    redisClient.get.mockImplementation((key: string) => {
      const id = key.replace('task:', '');
      return Promise.resolve(JSON.stringify({ id, traceId: 'trace-abc' }));
    });

    const tasks = await TaskQueue.getTasksByTraceId('trace-abc');
    expect(tasks).toHaveLength(3);
    expect(tasks.map(t => t.id)).toEqual(['t1', 't2', 't3']);
  });

  it('getTasksByTraceId returns empty array for unknown traceId', async () => {
    redisClient.sMembers.mockResolvedValue([]);
    const tasks = await TaskQueue.getTasksByTraceId('nonexistent');
    expect(tasks).toHaveLength(0);
  });

  // ── withTrace helper exported from logger ─────────────────────────────────

  it('withTrace is exported from logger.ts and is callable', () => {
    // withTrace must exist and return a child logger
    const tlog = withTrace({ trace_id: 'tr', span_id: 'sp' });
    expect(tlog).toBeDefined();
    expect(typeof tlog.info).toBe('function');
    expect(typeof tlog.error).toBe('function');
  });

  // ── TaskContext in executor carries traceId and parentSpanId ─────────────

  it('TaskExecutor passes traceId and parentSpanId to the handler context', async () => {
    const receivedContext: any = {};

    TaskExecutor.registerHandler('tracingHandler', async (_payload, ctx) => {
      Object.assign(receivedContext, ctx);
      return 'done';
    });

    const fakeTask = {
      id: 'exec-task-1',
      handler: 'tracingHandler',
      payload: {},
      traceId: 'trace-exec',
      parentSpanId: 'parent-exec',
      timeout: 5000,
      retries: 0,
    };

    jest.spyOn(require('../../src/services/worker-pool').WorkerPool, 'updateWorkerStatus').mockResolvedValue(undefined);
    jest.spyOn(require('../../src/services/worker-pool').WorkerPool, 'completeTask').mockResolvedValue(undefined);
    jest.spyOn(require('../../src/services/worker-pool').WorkerPool, 'getWorker').mockResolvedValue({ currentTasks: 0 });
    jest.spyOn(require('../../src/services/task-queue').TaskQueue, 'updateTaskStatus').mockResolvedValue(undefined);

    await TaskExecutor.execute('worker-1', fakeTask as any);

    expect(receivedContext.taskId).toBe('exec-task-1');
    expect(receivedContext.traceId).toBe('trace-exec');
    expect(receivedContext.parentSpanId).toBe('parent-exec');

    TaskExecutor.clearHandlers();
  });
});
