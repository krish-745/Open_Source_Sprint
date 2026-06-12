import { TaskQueue } from '../task-queue';
import * as redis from '../redis';
import { Task } from '../../types';

jest.mock('../redis');

const mockedGetRedisClient = redis.getRedisClient as jest.Mock;

function buildTask(overrides: Partial<Task> = {}): Task {
  return {
    id: 'task',
    name: 'demo',
    description: 'Task: demo',
    priority: 'medium',
    status: 'queued',
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

afterEach(() => jest.clearAllMocks());

describe('TaskQueue atomic dispatch (Consensus vs Normal)', () => {
  it('pops a normal task immediately (returns once)', async () => {
    const store: Record<string, string> = {
      'task:normal': JSON.stringify(buildTask({ id: 'normal' })),
    };
    
    const client: any = {
      zRange: jest.fn().mockResolvedValue(['normal']),
      get: jest.fn((k: string) => Promise.resolve(store[k] ?? null)),
      // First call simulates successful zRem (atomic acquire), second call simulates someone else already took it
      zRem: jest.fn().mockResolvedValueOnce(1).mockResolvedValueOnce(0),
    };
    mockedGetRedisClient.mockReturnValue(client);

    // Worker 1 fetches task
    const t1 = await TaskQueue.getNextTask('default');
    expect(t1?.id).toBe('normal');
    expect(client.zRem).toHaveBeenCalledWith('queue:default', 'normal');

    // Worker 2 fetches task concurrently (sees it in zRange, but fails zRem)
    const t2 = await TaskQueue.getNextTask('default');
    expect(t2).toBeNull();
  });

  it('allows multiple workers to fetch a consensus task up to the quorum', async () => {
    const store: Record<string, string> = {
      'task:consensus': JSON.stringify(buildTask({ 
        id: 'consensus',
        consensus: { workers: 3, strategy: 'majority' } 
      })),
    };
    
    // Simulate hIncrBy incrementing atomically
    let currentCount = 0;
    
    const client: any = {
      zRange: jest.fn().mockResolvedValue(['consensus']),
      get: jest.fn((k: string) => Promise.resolve(store[k] ?? null)),
      hIncrBy: jest.fn().mockImplementation(() => {
        currentCount++;
        return Promise.resolve(currentCount);
      }),
      zRem: jest.fn().mockResolvedValue(1),
      hDel: jest.fn().mockResolvedValue(1),
    };
    mockedGetRedisClient.mockReturnValue(client);

    // Worker 1
    const t1 = await TaskQueue.getNextTask('default');
    expect(t1?.id).toBe('consensus');
    expect(client.hIncrBy).toHaveBeenCalledWith('queue:default:dispatch', 'consensus', 1);
    expect(client.zRem).not.toHaveBeenCalled();

    // Worker 2
    const t2 = await TaskQueue.getNextTask('default');
    expect(t2?.id).toBe('consensus');
    expect(client.zRem).not.toHaveBeenCalled();

    // Worker 3 (Final worker for consensus)
    const t3 = await TaskQueue.getNextTask('default');
    expect(t3?.id).toBe('consensus');
    expect(client.zRem).toHaveBeenCalledWith('queue:default', 'consensus');

    // Worker 4 (Too late)
    const t4 = await TaskQueue.getNextTask('default');
    expect(t4).toBeNull(); // Because count is now 4, which is > consensus.workers
  });
});
