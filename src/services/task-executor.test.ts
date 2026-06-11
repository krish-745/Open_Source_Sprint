import { TaskExecutor } from './task-executor';
import { WorkerPool } from './worker-pool';
import { TaskQueue } from './task-queue';
import { Task } from '../types';

jest.mock('./worker-pool');
jest.mock('./task-queue');

const mockedWorkerPool = WorkerPool as jest.Mocked<typeof WorkerPool>;
const mockedTaskQueue = TaskQueue as jest.Mocked<typeof TaskQueue>;

function buildTask(overrides: Partial<Task> = {}): Task {
  return {
    id: 'task-1',
    name: 'demo',
    description: 'Task: demo',
    priority: 'medium',
    status: 'pending',
    handler: 'noop',
    payload: {},
    retries: 0,
    maxRetries: 3,
    timeout: 30000,
    createdAt: new Date(),
    queue: 'default',
    dependencies: [],
    tags: [],
    metadata: {},
    ...overrides,
  };
}

describe('TaskExecutor.execute worker status', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    TaskExecutor.clearHandlers();
    mockedTaskQueue.updateTaskStatus.mockResolvedValue(undefined as any);
    mockedWorkerPool.updateWorkerStatus.mockResolvedValue(undefined as any);
    mockedWorkerPool.completeTask.mockResolvedValue(undefined as any);
    TaskExecutor.registerHandler('noop', async () => 'ok');
  });

  it('keeps the worker busy when other tasks are still running after completion', async () => {
    mockedWorkerPool.getWorker.mockResolvedValue({ currentTasks: 2 } as any);

    await TaskExecutor.execute('w1', buildTask());

    expect(mockedWorkerPool.updateWorkerStatus).toHaveBeenLastCalledWith('w1', 'busy');
  });

  it('sets the worker idle when no tasks remain', async () => {
    mockedWorkerPool.getWorker.mockResolvedValue({ currentTasks: 0 } as any);

    await TaskExecutor.execute('w1', buildTask());

    expect(mockedWorkerPool.updateWorkerStatus).toHaveBeenLastCalledWith('w1', 'idle');
  });

  it('does not mark the task as completed if it was preempted by another worker', async () => {
    mockedWorkerPool.getWorker.mockResolvedValue({ currentTasks: 0 } as any);
    
    // Simulate that the task has been preempted and its workerId cleared or changed
    mockedTaskQueue.getTask.mockResolvedValue(buildTask({ workerId: undefined }));

    await TaskExecutor.execute('w1', buildTask());

    // Should NOT call updateTaskStatus with 'completed'
    expect(mockedTaskQueue.updateTaskStatus).not.toHaveBeenCalledWith('task-1', 'completed', expect.any(Object));
    // Should still call completeTask to decrement the worker's internal tasks counter
    expect(mockedWorkerPool.completeTask).toHaveBeenCalledWith('w1', 'task-1', expect.any(Object));
  });

  it('marks the task as completed if it was NOT preempted', async () => {
    mockedWorkerPool.getWorker.mockResolvedValue({ currentTasks: 0 } as any);
    
    // Simulate that the task is still assigned to this worker
    mockedTaskQueue.getTask.mockResolvedValue(buildTask({ workerId: 'w1' }));

    await TaskExecutor.execute('w1', buildTask());

    // Should call updateTaskStatus with 'completed'
    expect(mockedTaskQueue.updateTaskStatus).toHaveBeenCalledWith('task-1', 'completed', expect.any(Object));
    expect(mockedWorkerPool.completeTask).toHaveBeenCalledWith('w1', 'task-1', expect.any(Object));
  });
});
