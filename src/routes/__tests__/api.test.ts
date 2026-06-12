import { Request, Response } from 'express';
import router from '../api';
import { TaskQueue, CircularDependencyError } from '../../services/task-queue';
import { TaskExecutor } from '../../services/task-executor';

describe('API Routes - POST /tasks cycle handling', () => {
  let mockReq: Partial<Request>;
  let mockRes: Partial<Response>;
  let jsonMock: jest.Mock;
  let statusMock: jest.Mock;
  let createTaskSpy: jest.SpyInstance;

  beforeAll(() => {
    TaskExecutor.registerHandler('testHandler', async () => {});
  });

  afterAll(() => {
    TaskExecutor.clearHandlers();
  });

  beforeEach(() => {
    jsonMock = jest.fn();
    statusMock = jest.fn().mockReturnValue({ json: jsonMock });
    mockRes = {
      status: statusMock,
      json: jsonMock,
      on: jest.fn(),
    };
    createTaskSpy = jest.spyOn(TaskQueue, 'createTask');
    jest.clearAllMocks();
  });

  afterEach(() => {
    createTaskSpy.mockRestore();
  });

  it('should return 400 Bad Request if dependencies is not an array', async () => {
    mockReq = {
      method: 'POST',
      url: '/tasks',
      path: '/tasks',
      body: {
        name: 'taskA',
        handler: 'testHandler',
        dependencies: 'not-an-array'
      }
    };

    await new Promise<void>((resolve) => {
      router(mockReq as Request, mockRes as Response, () => {
        resolve();
      });
      setTimeout(resolve, 50);
    });

    expect(statusMock).toHaveBeenCalledWith(400);
    expect(jsonMock).toHaveBeenCalledWith({ error: 'Dependencies must be an array of task IDs' });
  });

  it('should return 400 Bad Request and cycle details on CircularDependencyError', async () => {
    const cycle = ['A', 'B', 'A'];
    createTaskSpy.mockRejectedValue(new CircularDependencyError(cycle));

    mockReq = {
      method: 'POST',
      url: '/tasks',
      path: '/tasks',
      body: {
        name: 'taskA',
        handler: 'testHandler',
        dependencies: ['B']
      }
    };

    await new Promise<void>((resolve) => {
      router(mockReq as Request, mockRes as Response, () => {
        resolve();
      });
      setTimeout(resolve, 50);
    });

    expect(statusMock).toHaveBeenCalledWith(400);
    expect(jsonMock).toHaveBeenCalledWith({
      error: 'Circular dependency detected: A -> B -> A',
      cycle: ['A', 'B', 'A']
    });
  });
});
