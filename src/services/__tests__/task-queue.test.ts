import { TaskQueue } from '../task-queue';
import { getRedisClient } from '../redis';

// Mock the Redis client using an in-memory store
const store: Record<string, string> = {};
const mockRedisClient = {
  get: jest.fn().mockImplementation(async (key: string) => store[key] || null),
  set: jest.fn().mockImplementation(async (key: string, value: string) => {
    store[key] = value;
    return 'OK';
  }),
  zAdd: jest.fn().mockResolvedValue(1),
  hIncrBy: jest.fn().mockResolvedValue(1),
};

jest.mock('../redis', () => ({
  getRedisClient: () => mockRedisClient,
}));

describe('TaskQueue - createTask payload validation', () => {
  beforeEach(() => {
    for (const key in store) {
      delete store[key];
    }
    jest.clearAllMocks();
  });

  it('should create a task with a valid object payload', async () => {
    const payload = { key: 'value' };
    const task = await TaskQueue.createTask('Test Task', 'testHandler', payload);
    
    expect(task).toBeDefined();
    expect(task.payload).toEqual(payload);
  });

  it('should default null payload to an empty object', async () => {
    const task = await TaskQueue.createTask('Test Task', 'testHandler', null);
    
    expect(task).toBeDefined();
    expect(task.payload).toEqual({});
  });

  it('should default undefined payload to an empty object', async () => {
    const task = await TaskQueue.createTask('Test Task', 'testHandler', undefined);
    
    expect(task).toBeDefined();
    expect(task.payload).toEqual({});
  });

  it('should throw an error if payload is a string', async () => {
    await expect(
      TaskQueue.createTask('Test Task', 'testHandler', 'invalid-string' as any)
    ).rejects.toThrow('Payload must be a valid object');
  });

  it('should throw an error if payload is an array', async () => {
    await expect(
      TaskQueue.createTask('Test Task', 'testHandler', [1, 2, 3] as any)
    ).rejects.toThrow('Payload must be a valid object');
  });

  it('should throw an error if payload is a number', async () => {
    await expect(
      TaskQueue.createTask('Test Task', 'testHandler', 123 as any)
    ).rejects.toThrow('Payload must be a valid object');
  });
});
