import { RedisConnectionPool } from './redis';

function fakeClient() {
  return { quit: jest.fn().mockResolvedValue('OK') } as any;
}

describe('RedisConnectionPool', () => {
  it('warms up to the minimum size', async () => {
    const factory = jest.fn(async () => fakeClient());
    const pool = new RedisConnectionPool(factory, 3, 5);
    await pool.warmUp();
    expect(factory).toHaveBeenCalledTimes(3);
    expect(pool.getStats().idle).toBe(3);
  });

  it('creates connections up to max, then queues acquire requests', async () => {
    const factory = jest.fn(async () => fakeClient());
    const pool = new RedisConnectionPool(factory, 0, 2);

    const c1 = await pool.acquire();
    const c2 = await pool.acquire();
    expect(pool.getStats().inUse).toBe(2);
    expect(factory).toHaveBeenCalledTimes(2);

    // Pool exhausted -> this acquire should queue, not create a 3rd connection.
    let resolved = false;
    const pending = pool.acquire().then((c) => {
      resolved = true;
      return c;
    });
    await Promise.resolve();
    expect(resolved).toBe(false);
    expect(pool.getStats().waiting).toBe(1);
    expect(factory).toHaveBeenCalledTimes(2);

    // Releasing one hands it to the waiter.
    pool.release(c1);
    const c3 = await pending;
    expect(resolved).toBe(true);
    expect(c3).toBe(c1);

    pool.release(c2);
    pool.release(c3);
  });

  it('reports utilization', async () => {
    const pool = new RedisConnectionPool(async () => fakeClient(), 0, 4);
    await pool.acquire();
    await pool.acquire();
    const stats = pool.getStats();
    expect(stats.inUse).toBe(2);
    expect(stats.max).toBe(4);
    expect(stats.utilization).toBe(0.5);
  });

  it('withConnection releases automatically', async () => {
    const pool = new RedisConnectionPool(async () => fakeClient(), 0, 1);
    const result = await pool.withConnection(async () => 'done');
    expect(result).toBe('done');
    expect(pool.getStats().inUse).toBe(0);
    expect(pool.getStats().idle).toBe(1);
  });

  it('rejects an invalid pool configuration', () => {
    expect(() => new RedisConnectionPool(async () => fakeClient(), 5, 2)).toThrow();
  });
});
