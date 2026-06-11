import { TaskQueue } from '../task-queue';
import { getRedisClient } from '../redis';

const store = new Map();
const mockClient = {
  set: jest.fn(async (key: string, value: string) => store.set(key, value)),
  get: jest.fn(async (key: string) => store.get(key)),
  zAdd: jest.fn(),
  hIncrBy: jest.fn(),
  zCard: jest.fn(),
  hGetAll: jest.fn(),
  zRange: jest.fn(),
  del: jest.fn(async (key: string) => store.delete(key)),
  zRem: jest.fn(),
  lPush: jest.fn(),
};

jest.mock('../redis', () => ({
  getRedisClient: jest.fn(() => mockClient),
}));

describe('TaskQueue', () => {
  let redisClient: any;

  beforeEach(() => {
    redisClient = getRedisClient();
    jest.clearAllMocks();
    store.clear();
    // Restore mock implementations in case a test used .mockResolvedValue()
    redisClient.get.mockImplementation(async (key: string) => store.get(key));
    redisClient.set.mockImplementation(async (key: string, value: string) => store.set(key, value));
  });

  describe('retryTask (Fix #18)', () => {
    it('should strictly delete the error field, not set it to undefined', async () => {
      const mockTask = {
        id: 'task-1',
        name: 'failing-task',
        status: 'failed',
        retries: 0,
        maxRetries: 3,
        error: 'Connection timed out',
        queue: 'default',
        priority: 'medium',
      };

      redisClient.get.mockResolvedValueOnce(JSON.stringify(mockTask));

      await TaskQueue.retryTask('task-1');

      const setCall = redisClient.set.mock.calls[0];
      expect(setCall).toBeDefined();

      const savedTask = JSON.parse(setCall[1]);

      // error must be completely absent from the serialized object
      expect(Object.keys(savedTask)).not.toContain('error');
      expect(savedTask.error).toBeUndefined();
    });

    it('should increment retries and set status to retry', async () => {
      const mockTask = {
        id: 'task-2',
        name: 'failing-task',
        status: 'failed',
        retries: 1,
        maxRetries: 3,
        error: 'Handler crashed',
        queue: 'default',
        priority: 'high',
      };

      redisClient.get.mockResolvedValueOnce(JSON.stringify(mockTask));

      await TaskQueue.retryTask('task-2');

      const savedTask = JSON.parse(redisClient.set.mock.calls[0][1]);
      expect(savedTask.status).toBe('retry');
      expect(savedTask.retries).toBe(2);
    });

    it('should move to dead letter queue and return false when maxRetries is exhausted', async () => {
      const mockTask = {
        id: 'task-3',
        name: 'failing-task',
        status: 'failed',
        retries: 3,
        maxRetries: 3,
        error: 'Permanent failure',
        queue: 'default',
        priority: 'low',
      };

      redisClient.get.mockResolvedValueOnce(JSON.stringify(mockTask));

      const result = await TaskQueue.retryTask('task-3');

      expect(result).toBe(false);
      expect(redisClient.set).not.toHaveBeenCalled();
    });
  });

  describe('createTask (Fix #22)', () => {
    it('should reject new tasks when queue size exceeds MAX_QUEUE_SIZE limit', async () => {
      // Configure max size to 100
      process.env.MAX_QUEUE_SIZE = '100';
      
      // Simulate queue already having 100 tasks
      redisClient.zCard.mockResolvedValueOnce(100);
      
      await expect(
        TaskQueue.createTask('test-task', 'test-handler', {})
      ).rejects.toThrow(/Queue default exceeds maximum size of 100/i);
    });

    it('should allow task creation when queue size is below MAX_QUEUE_SIZE limit', async () => {
      process.env.MAX_QUEUE_SIZE = '100';
      redisClient.zCard.mockResolvedValueOnce(99);
      redisClient.zAdd.mockResolvedValueOnce(1); // Mock adding to set

      const task = await TaskQueue.createTask('test-task', 'test-handler', {});
      
      expect(task).toBeDefined();
      expect(task.name).toBe('test-task');
    });
  });

  describe('Task Result Compression (Issue #10)', () => {
    it('should compress large task results (>10KB) before saving to Redis', async () => {
      const largeResult = 'A'.repeat(15 * 1024);
      const task = await TaskQueue.createTask('compression_test', 'test_handler', { test: true });
      
      await TaskQueue.updateTaskStatus(task.id, 'completed', { result: largeResult });
      
      const rawData = store.get(`task:${task.id}`);
      expect(rawData).toBeDefined();
      expect(rawData.length).toBeLessThan(1024 * 10);
      expect(rawData).toContain('__gz_json_b64__:');
    });

    it('should transparently decompress large task results on retrieval', async () => {
      const largeResult = 'B'.repeat(12 * 1024);
      const task = await TaskQueue.createTask('decompression_test', 'test_handler', { test: true });
      
      await TaskQueue.updateTaskStatus(task.id, 'completed', { result: largeResult });
      
      const retrievedTask = await TaskQueue.getTask(task.id);
      expect(retrievedTask).toBeDefined();
      expect(retrievedTask!.result).toBe(largeResult);
    });

    it('should correctly compress and decompress large object results', async () => {
      const largeObject = {
        key: 'value',
        data: 'C'.repeat(15 * 1024)
      };
      const task = await TaskQueue.createTask('obj_compression_test', 'test_handler', { test: true });
      
      await TaskQueue.updateTaskStatus(task.id, 'completed', { result: largeObject });
      
      const rawData = store.get(`task:${task.id}`);
      expect(rawData).toContain('__gz_json_b64__:');
      
      const retrievedTask = await TaskQueue.getTask(task.id);
      expect(retrievedTask!.result).toEqual(largeObject);
    });

    it('should store small task results uncompressed', async () => {
      const smallResult = 'D'.repeat(5 * 1024);
      const task = await TaskQueue.createTask('small_test', 'test_handler', { test: true });
      
      await TaskQueue.updateTaskStatus(task.id, 'completed', { result: smallResult });
      
      const rawData = store.get(`task:${task.id}`);
      expect(rawData).toBeDefined();
      expect(rawData).not.toContain('__gz_json_b64__:');
      
      const retrievedTask = await TaskQueue.getTask(task.id);
      expect(retrievedTask!.result).toBe(smallResult);
    });

    it('should update queue compression metrics exactly once per result payload', async () => {
      const largeResult = 'E'.repeat(15 * 1024);
      const task = await TaskQueue.createTask('metrics_test', 'test_handler', { test: true }, { queueName: 'myqueue' });
      
      await TaskQueue.updateTaskStatus(task.id, 'completed', { result: largeResult });
      
      expect(mockClient.hIncrBy).toHaveBeenCalledWith('queue:myqueue:stats', 'totalCompressedTasks', 1);
      
      // Clear mock and trigger a retry or update that DOES NOT provide a new result
      mockClient.hIncrBy.mockClear();
      await TaskQueue.retryTask(task.id);
      
      // Should not re-compress or double-count metrics
      expect(mockClient.hIncrBy).not.toHaveBeenCalledWith('queue:myqueue:stats', 'totalCompressedTasks', 1);
    });

    it('should properly compress tasks moved to the Dead Letter Queue', async () => {
      const largeResult = 'F'.repeat(15 * 1024);
      const task = await TaskQueue.createTask('dlq_test', 'test_handler', { test: true }, { maxRetries: 1 });
      
      await TaskQueue.updateTaskStatus(task.id, 'processing', { result: largeResult });
      await TaskQueue.retryTask(task.id);
      await TaskQueue.retryTask(task.id); // Moves to DLQ
      
      expect(mockClient.lPush).toHaveBeenCalled();
      const pushedData = mockClient.lPush.mock.calls[0][1];
      expect(pushedData).toContain('__gz_json_b64__:');
    });

    it('should prevent prefix spoofing bypass by forcing compression on matching payloads', async () => {
      // Small payload that tries to spoof the compressed format
      const spoofedResult = '__gz_json_b64__:malicious_payload';
      const task = await TaskQueue.createTask('spoof_test', 'test_handler', { test: true });
      
      await TaskQueue.updateTaskStatus(task.id, 'completed', { result: spoofedResult });
      
      const rawData = store.get(`task:${task.id}`);
      // It should have been double-wrapped (compressed) despite being < 10KB
      expect(rawData).toContain('__gz_json_b64__:');
      
      const retrievedTask = await TaskQueue.getTask(task.id);
      // It should safely decode back to the spoofed string, neutralizing the attack
      expect(retrievedTask!.result).toBe(spoofedResult);
    });

    it('should safely fall back to raw string for unparseable legacy v1 compressed payloads', async () => {
      // Emulate old __gz_b64__: payload that was a primitive string
      // gzip of '"true"'
      const task = await TaskQueue.createTask('legacy_test', 'test_handler', { test: true });
      
      const rawTask = store.get(`task:${task.id}`);
      const taskObj = JSON.parse(rawTask);
      
      // Using util.promisify(zlib.gzip) logic directly for the test setup
      const zlib = require('zlib');
      const compressed = zlib.gzipSync('true');
      taskObj.result = `__gz_b64__:${compressed.toString('base64')}`;
      store.set(`task:${task.id}`, JSON.stringify(taskObj));
      
      const retrievedTask = await TaskQueue.getTask(task.id);
      // Should fall back to string 'true', preventing morphing into boolean `true`
      expect(retrievedTask!.result).toBe('true');
    });
  });
});
