import {
  describe,
  it,
  beforeAll,
  beforeEach,
  afterAll,
  expect,
} from '@jest/globals';

import {
  initializeRedis,
  closeRedis,
  getRedisClient,
} from '../services/redis';

import { TaskQueue } from '../services/task-queue';
import { TaskExecutor } from '../services/task-executor';
import { WorkerPool } from '../services/worker-pool';

describe('Task System E2E Tests', () => {
  let workerId: string;

  beforeAll(async () => {
    await initializeRedis(
      process.env.REDIS_URL ||
        'redis://localhost:6379'
    );

    TaskExecutor.registerHandler(
      'success-handler',
      async (payload) => ({
        processed: true,
        payload,
      })
    );

    TaskExecutor.registerHandler(
      'timeout-handler',
      async () => {
        await new Promise((r) =>
          setTimeout(r, 2000)
        );

        return true;
      }
    );

    TaskExecutor.registerHandler(
      'failure-handler',
      async () => {
        throw new Error(
          'Intentional failure'
        );
      }
    );

    const worker =
      await WorkerPool.registerWorker(
        'e2e-worker',
        [
          'success-handler',
          'timeout-handler',
          'failure-handler',
        ]
      );

    workerId = worker.id;
  });

  beforeEach(async () => {
  const client = getRedisClient();

  const taskKeys =
    await client.keys('task:*');

  if (taskKeys.length) {
    await client.del(taskKeys);
  }

  const queueKeys =
    await client.keys('queue:*');

  if (queueKeys.length) {
    await client.del(queueKeys);
  }

  await client.del('tasks:index');
  await client.del('dlq:tasks');
});

  afterAll(async () => {
  try {
    TaskExecutor.clearHandlers();

    await closeRedis();
  } catch (err) {
    console.error(err);
  }
});

  /**
   * Create → Queue → Assign → Execute → Complete
   */
  it('should complete full task lifecycle', async () => {
    const task =
      await TaskQueue.createTask(
        'e2e-task',
        'success-handler',
        {
          value: 123,
        }
      );

    const queuedTask =
      await TaskQueue.getTask(task.id);

    expect(queuedTask).toBeDefined();
    expect(queuedTask?.status).toBe(
      'pending'
    );

    await WorkerPool.assignTask(
      workerId,
      task
    );

    await TaskExecutor.execute(
      workerId,
      task
    );

    const completed =
      await TaskQueue.getTask(
        task.id
      );

    expect(
      completed?.status
    ).toBe('completed');

    expect(
      completed?.result
    ).toBeDefined();

    expect(
      completed?.completedAt
    ).toBeDefined();
  });

  /**
   * Retry Flow
   */
  it('should retry failed task', async () => {
    const task =
      await TaskQueue.createTask(
        'retry-task',
        'failure-handler',
        {},
        {
          maxRetries: 2,
        }
      );

    await TaskExecutor.execute(
      workerId,
      task
    );

    const updated =
      await TaskQueue.getTask(
        task.id
      );

    expect(
      updated?.retries
    ).toBeGreaterThan(0);

    expect(updated?.status).toBe(
      'retry'
    );
  });

  /**
   * Dependency Flow
   */
  it('should respect dependencies', async () => {
    const parent =
      await TaskQueue.createTask(
        'parent',
        'success-handler',
        {}
      );

    const child =
      await TaskQueue.createTask(
        'child',
        'success-handler',
        {},
        {
          dependencies: [
            parent.id,
          ],
        }
      );

    const childTask =
      await TaskQueue.getTask(
        child.id
      );

    expect(
      childTask?.dependencies
    ).toContain(parent.id);

    await TaskExecutor.execute(
      workerId,
      parent
    );

    const completedParent =
      await TaskQueue.getTask(
        parent.id
      );

    expect(
      completedParent?.status
    ).toBe('completed');

    const refreshedChild =
      await TaskQueue.getTask(
        child.id
      );

    expect(
      refreshedChild
    ).toBeDefined();
  });

  /**
   * Timeout Flow
   */
  it(
    'should timeout long running task',
    async () => {
      const task =
        await TaskQueue.createTask(
          'timeout-task',
          'timeout-handler',
          {},
          {
            timeout: 500,
          }
        );

      await TaskExecutor.execute(
        workerId,
        task
      );

      const updated =
        await TaskQueue.getTask(
          task.id
        );

      expect(
        updated?.status
      ).not.toBe('completed');

      expect(
        ['retry', 'failed']
      ).toContain(
        updated?.status as string
      );
    }
  );

  /**
   * Error Scenario
   */
  it(
    'should handle execution errors',
    async () => {
      const task =
        await TaskQueue.createTask(
          'error-task',
          'failure-handler',
          {}
        );

      await TaskExecutor.execute(
        workerId,
        task
      );

      const updated =
        await TaskQueue.getTask(
          task.id
        );

      expect(
        ['retry', 'failed']
      ).toContain(
        updated?.status as string
      );
    }
  );
});