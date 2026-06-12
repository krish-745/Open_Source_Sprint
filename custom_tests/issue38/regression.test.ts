import { TaskQueue } from '../../src/services/task-queue';
import { TaskScheduler } from '../../src/services/task-scheduler';
import { MetricsCollector } from '../../src/services/metrics-collector';
import { getRedisClient, initializeRedis, closeRedis } from '../../src/services/redis';
import { WorkerPool } from '../../src/services/worker-pool';

describe('Issue #38: Task Priorities with SLA', () => {
  beforeAll(async () => {
    await initializeRedis('redis://localhost:6379');
    const client = getRedisClient();
    await client.flushAll();
  });

  afterAll(async () => {
    await closeRedis();
  });

  it('should enforce SLA and preempt low-priority tasks when high-priority is at risk', async () => {
    // 1. Create a low-priority task and set it to processing
    const lowTask = await TaskQueue.createTask('low-task', 'handler1', {}, { priority: 'low' });
    const worker = await WorkerPool.registerWorker('test-worker', ['handler1']);
    await WorkerPool.assignTask(worker.id, lowTask);
    await TaskQueue.updateTaskStatus(lowTask.id, 'processing', { workerId: worker.id });

    // 2. Create a high-priority task with a createdAt artificially in the past to put its SLA at risk
    const highTask = await TaskQueue.createTask('high-task', 'handler2', {}, { priority: 'high' });
    const client = getRedisClient();
    
    // SLA for high priority is 15000ms. We set createdAt to 20000ms ago.
    const pastTime = new Date(Date.now() - 20000).toISOString();
    highTask.createdAt = new Date(pastTime);
    await client.set(`task:${highTask.id}`, JSON.stringify(highTask));

    // 3. Trigger SLA enforcement (this is what the scheduler should run periodically)
    // We expect the new method enforceSLA to be on TaskScheduler or we'll trigger it somehow
    if ((TaskScheduler as any).enforceSLA) {
      await (TaskScheduler as any).enforceSLA();
    } else {
      throw new Error("enforceSLA method not implemented");
    }

    // 4. Verify the low-priority task was preempted (moved back to pending)
    const updatedLowTask = await TaskQueue.getTask(lowTask.id);
    expect(updatedLowTask?.status).toBe('pending');

    // 5. Verify SLA violation metrics were tracked
    const snapshot = await MetricsCollector.captureSnapshot();
    expect((snapshot.system as any).slaViolations).toBeDefined();
    expect((snapshot.system as any).slaViolations.high).toBeGreaterThanOrEqual(1);
  });
});
