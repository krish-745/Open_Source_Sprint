import { TaskQueue } from '../task-queue';
import { TaskScheduler } from '../task-scheduler';
import { MetricsCollector } from '../metrics-collector';
import { getRedisClient, initializeRedis, closeRedis } from '../redis';
import { WorkerPool } from '../worker-pool';

describe('TaskScheduler - SLA Enforcement', () => {
  beforeAll(async () => {
    await initializeRedis('redis://localhost:6379');
    const client = getRedisClient();
    await client.flushAll();
  });

  afterAll(async () => {
    await closeRedis();
  });

  beforeEach(async () => {
    const client = getRedisClient();
    await client.flushAll();
  });

  it('should not preempt low-priority tasks if high-priority task SLA is not violated', async () => {
    const lowTask = await TaskQueue.createTask('low-task', 'handler1', {}, { priority: 'low' });
    const worker = await WorkerPool.registerWorker('test-worker', ['handler1']);
    await WorkerPool.assignTask(worker.id, lowTask);
    await TaskQueue.updateTaskStatus(lowTask.id, 'processing', { workerId: worker.id });

    const highTask = await TaskQueue.createTask('high-task', 'handler2', {}, { priority: 'high' });
    
    // Default createdAt is now, so it shouldn't violate the 15s SLA

    await TaskScheduler.enforceSLA();

    const updatedLowTask = await TaskQueue.getTask(lowTask.id);
    expect(updatedLowTask?.status).toBe('processing'); // Remains processing

    const snapshot = await MetricsCollector.captureSnapshot();
    const sys = snapshot.system as any;
    expect(sys.slaViolations).toBeDefined();
    expect(sys.slaViolations.high).toBe(0);
  });

  it('should enforce SLA and preempt low-priority tasks when high-priority is at risk', async () => {
    const lowTask = await TaskQueue.createTask('low-task', 'handler1', {}, { priority: 'low' });
    const worker = await WorkerPool.registerWorker('test-worker', ['handler1']);
    await WorkerPool.assignTask(worker.id, lowTask);
    await TaskQueue.updateTaskStatus(lowTask.id, 'processing', { workerId: worker.id });

    const highTask = await TaskQueue.createTask('high-task', 'handler2', {}, { priority: 'high' });
    const client = getRedisClient();
    
    // Set createdAt 20000ms ago to violate the 15000ms SLA
    const pastTime = new Date(Date.now() - 20000).toISOString();
    highTask.createdAt = new Date(pastTime);
    await client.set(`task:${highTask.id}`, JSON.stringify(highTask));

    await TaskScheduler.enforceSLA();

    // Verify the low-priority task was preempted (moved back to pending)
    const updatedLowTask = await TaskQueue.getTask(lowTask.id);
    expect(updatedLowTask?.status).toBe('pending');

    // Verify SLA violation metrics were tracked
    const snapshot = await MetricsCollector.captureSnapshot();
    const sys = snapshot.system as any;
    expect(sys.slaViolations).toBeDefined();
    expect(sys.slaViolations.high).toBe(1);
  });

  it('should not preempt low-priority tasks if high-priority task SLA is violated but it is blocked by dependencies', async () => {
    const lowTask = await TaskQueue.createTask('low-task', 'handler1', {}, { priority: 'low' });
    const worker = await WorkerPool.registerWorker('test-worker', ['handler1']);
    await WorkerPool.assignTask(worker.id, lowTask);
    await TaskQueue.updateTaskStatus(lowTask.id, 'processing', { workerId: worker.id });

    const highTask = await TaskQueue.createTask('high-task', 'handler2', {}, { 
      priority: 'high',
      dependencies: ['some-uncompleted-task-id']
    });
    
    const client = getRedisClient();
    const pastTime = new Date(Date.now() - 20000).toISOString();
    highTask.createdAt = new Date(pastTime);
    await client.set(`task:${highTask.id}`, JSON.stringify(highTask));

    await TaskScheduler.enforceSLA();

    // Verify the low-priority task was NOT preempted because the high-priority task is blocked
    const updatedLowTask = await TaskQueue.getTask(lowTask.id);
    expect(updatedLowTask?.status).toBe('processing');
  });

  it('should not preempt low-priority tasks if high-priority task SLA is violated but it is scheduled for the future', async () => {
    const lowTask = await TaskQueue.createTask('low-task', 'handler1', {}, { priority: 'low' });
    const worker = await WorkerPool.registerWorker('test-worker', ['handler1']);
    await WorkerPool.assignTask(worker.id, lowTask);
    await TaskQueue.updateTaskStatus(lowTask.id, 'processing', { workerId: worker.id });

    const futureTime = new Date(Date.now() + 60000); // scheduled 1 min in future
    const highTask = await TaskQueue.createTask('high-task', 'handler2', {}, { 
      priority: 'high',
      scheduledFor: futureTime
    });
    
    const client = getRedisClient();
    const pastTime = new Date(Date.now() - 20000).toISOString();
    highTask.createdAt = new Date(pastTime);
    await client.set(`task:${highTask.id}`, JSON.stringify(highTask));

    await TaskScheduler.enforceSLA();

    // Verify the low-priority task was NOT preempted because the high-priority task is scheduled for future
    const updatedLowTask = await TaskQueue.getTask(lowTask.id);
    expect(updatedLowTask?.status).toBe('processing');
  });
});
