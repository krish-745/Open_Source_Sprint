import { WorkerPool } from './worker-pool';
import { TaskQueue } from './task-queue';
import * as redis from './redis';
import { Worker } from '../types';

jest.mock('./redis');
jest.mock('./task-queue');

const mockedGetRedisClient = redis.getRedisClient as jest.Mock;
const mockedTaskQueue = TaskQueue as jest.Mocked<typeof TaskQueue>;

function buildWorker(overrides: Partial<Worker> = {}): Worker {
  return {
    id: 'w1',
    name: 'worker',
    status: 'busy',
    handlers: ['noop'],
    maxConcurrent: 5,
    currentTasks: 2,
    totalProcessed: 0,
    totalFailed: 0,
    lastHeartbeat: new Date(),
    registeredAt: new Date(),
    version: '1.0.0',
    capacity: 40,
    tags: [],
    ...overrides,
  };
}

afterEach(() => jest.clearAllMocks());

describe('WorkerPool.handleWorkerDisconnect', () => {
  it('requeues in-flight tasks, clears assignment, and marks the worker offline', async () => {
    const store: Record<string, string> = {
      'worker:w1': JSON.stringify(buildWorker()),
    };
    const client: any = {
      lRange: jest.fn().mockResolvedValue(['t1', 't2']),
      del: jest.fn().mockResolvedValue(1),
      get: jest.fn((key: string) => Promise.resolve(store[key] ?? null)),
      set: jest.fn((key: string, value: string) => {
        store[key] = value;
        return Promise.resolve('OK');
      }),
    };
    mockedGetRedisClient.mockReturnValue(client);
    mockedTaskQueue.requeueTask.mockResolvedValue(true);

    const reassigned = await WorkerPool.handleWorkerDisconnect('w1');

    expect(reassigned).toBe(2);
    expect(mockedTaskQueue.requeueTask).toHaveBeenCalledWith('t1');
    expect(mockedTaskQueue.requeueTask).toHaveBeenCalledWith('t2');
    expect(client.del).toHaveBeenCalledWith('worker:w1:tasks');

    const saved = JSON.parse(store['worker:w1']);
    expect(saved.status).toBe('offline');
    expect(saved.currentTasks).toBe(0);
    expect(saved.capacity).toBe(0);
  });

  it('returns 0 when the worker had no in-flight tasks', async () => {
    const store: Record<string, string> = {
      'worker:w1': JSON.stringify(buildWorker({ currentTasks: 0 })),
    };
    const client: any = {
      lRange: jest.fn().mockResolvedValue([]),
      del: jest.fn().mockResolvedValue(0),
      get: jest.fn((key: string) => Promise.resolve(store[key] ?? null)),
      set: jest.fn().mockResolvedValue('OK'),
    };
    mockedGetRedisClient.mockReturnValue(client);

    const reassigned = await WorkerPool.handleWorkerDisconnect('w1');

    expect(reassigned).toBe(0);
    expect(mockedTaskQueue.requeueTask).not.toHaveBeenCalled();
  });
});
