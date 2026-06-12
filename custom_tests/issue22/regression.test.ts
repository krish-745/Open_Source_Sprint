import { TaskQueue } from '../../src/services/task-queue';
import { getRedisClient } from '../../src/services/redis';
import { Request, Response } from 'express';
import apiRouter from '../../src/routes/api';
import { TaskExecutor } from '../../src/services/task-executor';

jest.mock('../../src/services/redis', () => {
  const mockQueueSize = jest.fn().mockResolvedValue(0);
  const mClient = {
    set: jest.fn(),
    zAdd: jest.fn(),
    zCard: mockQueueSize,
    hGetAll: jest.fn().mockResolvedValue({}),
    hIncrBy: jest.fn(),
  };
  return { 
    getRedisClient: jest.fn(() => mClient),
    _mockQueueSize: mockQueueSize
  };
});

describe('Issue #22: Backpressure in Queue', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should reject new tasks when queue size exceeds MAX_QUEUE_SIZE limit', async () => {
    const { _mockQueueSize } = require('../../src/services/redis');
    
    // Simulate queue already having 100 tasks
    _mockQueueSize.mockResolvedValue(100);
    
    // Configure max size to 100
    process.env.MAX_QUEUE_SIZE = '100';

    // Trying to create another task should throw an error
    await expect(
      TaskQueue.createTask('test-task', 'test-handler', {})
    ).rejects.toThrow(/Queue.*exceeds maximum size/i);
  });

  it('API should return HTTP 429 when queue is full', async () => {
    const { _mockQueueSize } = require('../../src/services/redis');
    
    // Simulate queue full
    _mockQueueSize.mockResolvedValue(100);
    process.env.MAX_QUEUE_SIZE = '100';

    jest.spyOn(TaskExecutor, 'hasHandler').mockReturnValue(true);

    const req = {
      method: 'POST',
      url: '/tasks',
      body: {
        name: 'test-task',
        handler: 'test-handler',
        payload: {}
      }
    } as unknown as Request;

    const res = {
      status: jest.fn().mockReturnThis(),
      json: jest.fn(),
      on: jest.fn()
    } as unknown as Response;

    // We must find the router layer and invoke it manually, but Express router is a function.
    // An easier way is to mock TaskQueue.createTask to throw the specific error that the queue is full.
    // Wait, the API router catches error and currently returns 500.
    
    const next = jest.fn();
    // Express router does not return a promise that resolves when the handler finishes.
    // We need to wait a tick for the async handler to execute.
    apiRouter(req, res, next);
    
    await new Promise(resolve => setTimeout(resolve, 50));
    
    expect(res.status).toHaveBeenCalledWith(429);
    expect(res.json).toHaveBeenCalledWith(expect.objectContaining({
      error: expect.stringMatching(/exceeds maximum size/i)
    }));
  });
});
