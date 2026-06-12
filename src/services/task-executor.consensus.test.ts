import { TaskExecutor } from './task-executor';
import { TaskQueue } from './task-queue';
import { WorkerPool } from './worker-pool';
import { TaskHooks } from './task-hooks';
import { Task } from '../types';

jest.mock('./task-queue', () => ({
  TaskQueue: {
    updateTaskStatus: jest.fn(),
    retryTask: jest.fn(() => false),
  }
}));

jest.mock('./worker-pool', () => ({
  WorkerPool: {
    getWorker: jest.fn((id: string) => ({ id, name: id, maxConcurrent: 5, currentTasks: 1, capacity: 20 })),
    updateWorkerStatus: jest.fn(),
    calculateTaskCost: jest.fn(() => 0),
    completeTask: jest.fn(),
  }
}));

jest.mock('./task-hooks', () => ({
  TaskHooks: {
    emitTask: jest.fn(),
  }
}));

describe('TaskExecutor Consensus', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    TaskExecutor.clearHandlers();
  });

  const buildTask = (overrides: Partial<Task> = {}): Task => ({
    id: 't-consensus',
    name: 'test',
    description: '',
    priority: 'medium',
    status: 'pending',
    handler: 'testHandler',
    payload: {},
    retries: 0,
    maxRetries: 3,
    timeout: 1000,
    createdAt: new Date(),
    queue: 'default',
    dependencies: [],
    tags: [],
    metadata: {},
    quorum: {
      count: 3,
      strategy: 'all'
    },
    ...overrides,
  });

  it('should reach consensus with "all" strategy if all workers agree', async () => {
    TaskExecutor.registerHandler('testHandler', async () => {
      return { resultData: 42 }; // All workers return exactly this
    });

    const task = buildTask();
    await TaskExecutor.executeWithConsensus(['w1', 'w2', 'w3'], task);

    expect(TaskQueue.updateTaskStatus).toHaveBeenCalledWith('t-consensus', 'completed', expect.objectContaining({
      result: { resultData: 42 }
    }));

    // Should complete success for all workers
    expect(WorkerPool.completeTask).toHaveBeenCalledWith('w1', 't-consensus', expect.objectContaining({ success: true }));
    expect(WorkerPool.completeTask).toHaveBeenCalledWith('w2', 't-consensus', expect.objectContaining({ success: true }));
    expect(WorkerPool.completeTask).toHaveBeenCalledWith('w3', 't-consensus', expect.objectContaining({ success: true }));
  });

  it('should fail consensus with "all" strategy if one worker disagrees', async () => {
    let callCount = 0;
    TaskExecutor.registerHandler('testHandler', async () => {
      callCount++;
      return { resultData: callCount === 3 ? 99 : 42 }; // 3rd worker returns 99
    });

    const task = buildTask();
    await expect(TaskExecutor.executeWithConsensus(['w1', 'w2', 'w3'], task)).resolves.toBeUndefined(); // Caught internally and marked failed

    // It should have failed the task
    expect(TaskQueue.updateTaskStatus).toHaveBeenCalledWith('t-consensus', 'failed', expect.objectContaining({
      error: 'Consensus not reached'
    }));

    // All workers get failed status if the task fails consensus entirely
    expect(WorkerPool.completeTask).toHaveBeenCalledWith('w1', 't-consensus', expect.objectContaining({ success: false }));
  });

  it('should reach consensus with "majority" strategy if 2/3 agree', async () => {
    let callCount = 0;
    TaskExecutor.registerHandler('testHandler', async () => {
      callCount++;
      return { resultData: callCount === 3 ? 99 : 42 }; // 3rd worker disagrees
    });

    const task = buildTask({
      quorum: { count: 3, strategy: 'majority' }
    });

    await TaskExecutor.executeWithConsensus(['w1', 'w2', 'w3'], task);

    expect(TaskQueue.updateTaskStatus).toHaveBeenCalledWith('t-consensus', 'completed', expect.objectContaining({
      result: { resultData: 42 }
    }));

    // w1 and w2 succeeded, w3 failed (penalized)
    expect(WorkerPool.completeTask).toHaveBeenCalledWith('w1', 't-consensus', expect.objectContaining({ success: true }));
    expect(WorkerPool.completeTask).toHaveBeenCalledWith('w2', 't-consensus', expect.objectContaining({ success: true }));
    expect(WorkerPool.completeTask).toHaveBeenCalledWith('w3', 't-consensus', expect.objectContaining({ success: false }));
  });
});
