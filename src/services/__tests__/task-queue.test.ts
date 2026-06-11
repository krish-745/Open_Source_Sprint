import { TaskQueue } from '../task-queue';
import { getRedisClient } from '../redis';
import { Task } from '../../types';

// Mock the Redis client using an in-memory store
const store: Record<string, string> = {};
const mockRedisClient = {
  get: jest.fn().mockImplementation(async (key: string) => store[key] || null),
  set: jest.fn().mockImplementation(async (key: string, value: string) => {
    store[key] = value;
    return 'OK';
  }),
  zAdd: jest.fn().mockResolvedValue(1),
  lPush: jest.fn().mockResolvedValue(1),
  del: jest.fn().mockImplementation(async (key: string) => {
    delete store[key];
    return 1;
  }),
};

jest.mock('../redis', () => ({
  getRedisClient: () => mockRedisClient,
}));

describe('TaskQueue - retryTask', () => {
  beforeEach(() => {
    // Clear the store before each test
    for (const key in store) {
      delete store[key];
    }
    jest.clearAllMocks();
  });

  it('should successfully increment retries and delete error property on retry', async () => {
    const taskId = 'test-task-123';
    const task: Task = {
      id: taskId,
      name: 'Test Task',
      description: 'Task description',
      priority: 'medium',
      status: 'failed',
      handler: 'testHandler',
      payload: {},
      retries: 0,
      maxRetries: 3,
      timeout: 30000,
      createdAt: new Date(),
      queue: 'default',
      dependencies: [],
      tags: [],
      metadata: {},
      error: 'Task execution failed with error x',
    };

    // Store the task initially
    store[`task:${taskId}`] = JSON.stringify(task);

    const result = await TaskQueue.retryTask(taskId);
    expect(result).toBe(true);

    // Retrieve and parse the updated task
    const updatedTaskData = store[`task:${taskId}`];
    expect(updatedTaskData).toBeDefined();

    const updatedTask: Task = JSON.parse(updatedTaskData);
    expect(updatedTask.retries).toBe(1);
    expect(updatedTask.status).toBe('retry');
    
    // The error property should be completely removed, not just set to undefined
    expect(updatedTask.error).toBeUndefined();
    expect('error' in updatedTask).toBe(false);
  });
});
