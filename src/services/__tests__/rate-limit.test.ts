import { TaskQueue } from '../task-queue';
import { WorkerPool } from '../worker-pool';
import { initializeRedis, closeRedis, getRedisClient } from '../redis';

describe('Issue #12: Task Rate Limiting', () => {
  let client: any;

  beforeAll(async () => {
    client = await initializeRedis('redis://localhost:6379');
  });

  beforeEach(async () => {
    await client.flushAll();
  });

  afterAll(async () => {
    await closeRedis();
  });

  // Helper: try to assign N tasks from a queue to a worker, return { assigned, denied }
  async function tryAssignN(queueName: string, workerId: string, n: number) {
    let assigned = 0;
    let denied = 0;
    for (let i = 0; i < n; i++) {
      const task = await TaskQueue.getNextTask(queueName);
      if (task) {
        try {
          await WorkerPool.assignTask(workerId, task);
          assigned++;
        } catch (error: any) {
          if (error.message.includes('Rate limit exceeded')) {
            denied++;
          } else {
            throw error;
          }
        }
        // Remove from queue so getNextTask advances to the next task
        await client.zRem(`queue:${queueName}`, task.id);
      }
    }
    return { assigned, denied };
  }

  it('AC1+AC5: configureRateLimit stores the limit per queue and getRateLimit reads it back', async () => {
    await TaskQueue.configureRateLimit('queue-a', 5);
    await TaskQueue.configureRateLimit('queue-b', 10);

    expect(await TaskQueue.getRateLimit('queue-a')).toBe(5);
    expect(await TaskQueue.getRateLimit('queue-b')).toBe(10);

    // Queues are truly independent — changing one does not affect the other
    await TaskQueue.configureRateLimit('queue-a', 3);
    expect(await TaskQueue.getRateLimit('queue-a')).toBe(3);
    expect(await TaskQueue.getRateLimit('queue-b')).toBe(10); // unchanged
  });

  it('AC2: throttles task assignment exactly at the configured limit', async () => {
    const queueName = 'throttle-queue';
    const LIMIT = 2;

    // Create more tasks than the limit
    for (let i = 0; i < 6; i++) {
      await TaskQueue.createTask(`task-${i}`, 'dummy-handler', {}, { queueName });
    }
    await TaskQueue.configureRateLimit(queueName, LIMIT);

    const worker = await WorkerPool.registerWorker('worker-1', ['dummy-handler']);
    const { assigned, denied } = await tryAssignN(queueName, worker.id, 6);

    // Exactly LIMIT tasks should succeed — no more, no fewer
    expect(assigned).toBe(LIMIT);
    // The remaining 6 - LIMIT attempts must all be denied
    expect(denied).toBe(6 - LIMIT);
  });

  it('AC3: token bucket refills after 1 second allowing more assignments', async () => {
    const queueName = 'refill-queue';
    const LIMIT = 2;

    // Need enough tasks for both rounds:
    // Round 1: 4 attempts, each zRem'd → 4 tasks consumed from queue
    // Round 2: 4 attempts, needs 4 tasks still in queue → total 8 tasks minimum
    for (let i = 0; i < 10; i++) {
      await TaskQueue.createTask(`task-${i}`, 'dummy-handler', {}, { queueName });
    }
    await TaskQueue.configureRateLimit(queueName, LIMIT);

    const worker = await WorkerPool.registerWorker('worker-1', ['dummy-handler']);

    // Exhaust the initial tokens
    const round1 = await tryAssignN(queueName, worker.id, 4);
    expect(round1.assigned).toBe(LIMIT);
    expect(round1.denied).toBe(4 - LIMIT);

    // Wait slightly longer than 1 second for the bucket to fully refill
    await new Promise(resolve => setTimeout(resolve, 1100));

    // After refill, should be able to assign exactly LIMIT more tasks
    const round2 = await tryAssignN(queueName, worker.id, 4);
    expect(round2.assigned).toBe(LIMIT);
    expect(round2.denied).toBe(4 - LIMIT);
  });

  it('AC4: rate limit violations are tracked with the exact count', async () => {
    const queueName = 'metrics-queue';
    const LIMIT = 2;
    const ATTEMPTS = 5;

    for (let i = 0; i < ATTEMPTS; i++) {
      await TaskQueue.createTask(`task-${i}`, 'dummy-handler', {}, { queueName });
    }
    await TaskQueue.configureRateLimit(queueName, LIMIT);

    const worker = await WorkerPool.registerWorker('worker-1', ['dummy-handler']);
    await tryAssignN(queueName, worker.id, ATTEMPTS);

    const stats = await TaskQueue.getQueueStats(queueName);
    expect(stats).toHaveProperty('rateLimitViolations');
    // Exactly ATTEMPTS - LIMIT violations should be recorded
    expect(stats.rateLimitViolations).toBe(ATTEMPTS - LIMIT);
  });

  it('AC5: rate limit is per worker/queue — each worker gets its own independent token bucket', async () => {
    const queueName = 'shared-queue';
    const LIMIT = 2;

    for (let i = 0; i < 10; i++) {
      await TaskQueue.createTask(`task-${i}`, 'dummy-handler', {}, { queueName });
    }
    await TaskQueue.configureRateLimit(queueName, LIMIT);

    const worker1 = await WorkerPool.registerWorker('worker-1', ['dummy-handler']);
    const worker2 = await WorkerPool.registerWorker('worker-2', ['dummy-handler']);

    // Worker 1 exhausts its bucket
    const w1 = await tryAssignN(queueName, worker1.id, 5);
    expect(w1.assigned).toBe(LIMIT);
    expect(w1.denied).toBe(5 - LIMIT); // Worker 1 must be throttled after LIMIT tasks

    // Worker 2 immediately attempts — its bucket is independent, must still get LIMIT tasks
    const w2 = await tryAssignN(queueName, worker2.id, 5);
    expect(w2.assigned).toBe(LIMIT);   // NOT starved by worker 1
    expect(w2.denied).toBe(5 - LIMIT);
  });

  it('no rate limit configured → all task assignments are allowed', async () => {
    const queueName = 'unlimited-queue';

    for (let i = 0; i < 5; i++) {
      await TaskQueue.createTask(`task-${i}`, 'dummy-handler', {}, { queueName });
    }
    // Deliberately do NOT call configureRateLimit
    expect(await TaskQueue.getRateLimit(queueName)).toBeNull();

    const worker = await WorkerPool.registerWorker('worker-1', ['dummy-handler']);
    const { assigned, denied } = await tryAssignN(queueName, worker.id, 5);

    expect(assigned).toBe(5);
    expect(denied).toBe(0);
  });
});
