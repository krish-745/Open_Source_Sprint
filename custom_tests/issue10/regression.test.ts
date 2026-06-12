import { TaskQueue } from '../../src/services/task-queue';
import { getRedisClient } from '../../src/services/redis';
import { v4 as uuidv4 } from 'uuid';

const store = new Map();
const mockClient = {
  set: jest.fn(async (key: string, value: string) => store.set(key, value)),
  get: jest.fn(async (key: string) => store.get(key)),
  zAdd: jest.fn(),
  hIncrBy: jest.fn(),
  zCard: jest.fn(),
  hGetAll: jest.fn(),
  zRange: jest.fn(),
  del: jest.fn(),
  zRem: jest.fn(),
};

jest.mock('../../src/services/redis', () => {
  return {
    getRedisClient: jest.fn(() => mockClient),
  };
});

describe('Issue #10: Task Result Compression', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    store.clear();
  });

  it('should compress large task results (>10KB) before saving to Redis and decompress on retrieval', async () => {
    // Create a 15KB string payload
    const largeResult = 'A'.repeat(15 * 1024);
    
    // Create task
    const task = await TaskQueue.createTask('compression_test', 'test_handler', { test: true });
    
    // Update task with large result
    await TaskQueue.updateTaskStatus(task.id, 'completed', { result: largeResult });
    
    // 1. Verify the raw stored value is smaller than 15KB (meaning it was compressed)
    const rawData = store.get(`task:${task.id}`);
    expect(rawData).toBeDefined();
    expect(rawData!.length).toBeLessThan(1024 * 10); // Should be much smaller than 10KB after compression
    
    // 2. Verify that `getTask` transparently decompresses it
    const retrievedTask = await TaskQueue.getTask(task.id);
    expect(retrievedTask).toBeDefined();
    expect(retrievedTask!.result).toBe(largeResult);
  });
});
