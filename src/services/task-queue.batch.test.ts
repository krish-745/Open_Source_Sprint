import { TaskQueue } from './task-queue';
import * as redis from './redis';

jest.mock('./redis');

const mockedGetRedisClient = redis.getRedisClient as jest.Mock;

function makeClient() {
  return {
    set: jest.fn().mockResolvedValue('OK'),
    zAdd: jest.fn().mockResolvedValue(1),
    zCard: jest.fn().mockResolvedValue(0),
    hIncrBy: jest.fn().mockResolvedValue(1),
  } as any;
}

afterEach(() => jest.clearAllMocks());

describe('TaskQueue.createTasksBatch', () => {
  it('creates all tasks and returns them in order', async () => {
    const client = makeClient();
    mockedGetRedisClient.mockReturnValue(client);

    const created = await TaskQueue.createTasksBatch([
      { name: 'a', handler: 'h' },
      { name: 'b', handler: 'h' },
      { name: 'c', handler: 'h' },
    ]);

    expect(created).toHaveLength(3);
    expect(created.map((t) => t.name)).toEqual(['a', 'b', 'c']);
    expect(created.every((t) => typeof t.id === 'string')).toBe(true);
  });

  it('rejects the whole batch (creates nothing) if any task is invalid', async () => {
    const client = makeClient();
    mockedGetRedisClient.mockReturnValue(client);

    await expect(
      TaskQueue.createTasksBatch([
        { name: 'a', handler: 'h' },
        { name: '', handler: 'h' }, // invalid
      ])
    ).rejects.toThrow(/index 1/);

    // Validation happens before any write.
    expect(client.set).not.toHaveBeenCalled();
  });

  it('rejects an empty batch', async () => {
    mockedGetRedisClient.mockReturnValue(makeClient());
    await expect(TaskQueue.createTasksBatch([])).rejects.toThrow(/at least one/);
  });

  it('rejects a batch over the 1000-task limit', async () => {
    mockedGetRedisClient.mockReturnValue(makeClient());
    const big = Array.from({ length: 1001 }, (_, i) => ({ name: `t${i}`, handler: 'h' }));
    await expect(TaskQueue.createTasksBatch(big)).rejects.toThrow(/maximum of 1000/);
  });
});
