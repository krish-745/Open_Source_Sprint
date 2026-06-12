import { WorkerPool } from '../worker-pool';
import * as redis from '../redis';
import { Task } from '../../types';

jest.mock('../redis');

const mockedGetRedisClient = redis.getRedisClient as jest.Mock;

/**
 * Minimal in-memory Redis fake implementing the commands WorkerPool uses
 * (strings, sets, sorted sets, lists). Keeps the worker-pool operations under
 * test running end-to-end against shared state without requiring a live Redis.
 */
function makeFakeRedis() {
  const strings = new Map<string, string>();
  const sets = new Map<string, Set<string>>();
  const zsets = new Map<string, Map<string, number>>();
  const lists = new Map<string, string[]>();

  return {
    set: jest.fn(async (k: string, v: string) => {
      strings.set(k, v);
      return 'OK';
    }),
    get: jest.fn(async (k: string) => strings.get(k) ?? null),
    del: jest.fn(async (k: string) => {
      strings.delete(k);
      lists.delete(k);
      return 1;
    }),
    sAdd: jest.fn(async (k: string, m: string) => {
      const s = sets.get(k) ?? new Set<string>();
      s.add(m);
      sets.set(k, s);
      return 1;
    }),
    sMembers: jest.fn(async (k: string) => Array.from(sets.get(k) ?? [])),
    sRem: jest.fn(async (k: string, m: string) => {
      sets.get(k)?.delete(m);
      return 1;
    }),
    zAdd: jest.fn(async (k: string, { score, value }: { score: number; value: string }) => {
      const z = zsets.get(k) ?? new Map<string, number>();
      z.set(value, score);
      zsets.set(k, z);
      return 1;
    }),
    zRange: jest.fn(async (k: string, start: number, stop: number) => {
      const z = zsets.get(k);
      if (!z) return [];
      const ordered = [...z.entries()].sort((a, b) => a[1] - b[1]).map((e) => e[0]);
      return stop === -1 ? ordered.slice(start) : ordered.slice(start, stop + 1);
    }),
    zRem: jest.fn(async (k: string, m: string) => {
      zsets.get(k)?.delete(m);
      return 1;
    }),
    lPush: jest.fn(async (k: string, v: string) => {
      const l = lists.get(k) ?? [];
      l.unshift(v);
      lists.set(k, l);
      return l.length;
    }),
    lRem: jest.fn(async (k: string, _count: number, v: string) => {
      const l = lists.get(k) ?? [];
      const i = l.indexOf(v);
      if (i >= 0) l.splice(i, 1);
      return 1;
    }),
    lRange: jest.fn(async (k: string, start: number, stop: number) => {
      const l = lists.get(k) ?? [];
      return stop === -1 ? l.slice(start) : l.slice(start, stop + 1);
    }),
    __strings: strings,
  } as any;
}

function buildTask(id: string): Task {
  return {
    id,
    name: 'demo',
    description: 'Task: demo',
    priority: 'medium',
    status: 'pending',
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
  };
}

let client: ReturnType<typeof makeFakeRedis>;

beforeEach(() => {
  client = makeFakeRedis();
  mockedGetRedisClient.mockReturnValue(client);
});

describe('WorkerPool registration and lookup', () => {
  it('registers a worker, stores it, and maps its handlers', async () => {
    const worker = await WorkerPool.registerWorker('w', ['noop', 'email'], { maxConcurrent: 3 });

    const fetched = await WorkerPool.getWorker(worker.id);
    expect(fetched?.id).toBe(worker.id);
    expect(fetched?.maxConcurrent).toBe(3);

    const forHandler = await client.sMembers('worker:handlers:map:noop');
    expect(forHandler).toContain(worker.id);
  });

  it('lists available workers for a handler', async () => {
    const worker = await WorkerPool.registerWorker('w', ['noop']);
    const available = await WorkerPool.getAvailableWorkers('noop');
    expect(available.map((w) => w.id)).toContain(worker.id);
  });
});

describe('WorkerPool task assignment lifecycle', () => {
  it('increments currentTasks on assign and decrements + counts on complete', async () => {
    const worker = await WorkerPool.registerWorker('w', ['noop'], { maxConcurrent: 5 });

    await WorkerPool.assignTask(worker.id, buildTask('t1'));
    let state = await WorkerPool.getWorker(worker.id);
    expect(state?.currentTasks).toBe(1);
    expect(state?.capacity).toBe(20);

    await WorkerPool.completeTask(worker.id, 't1', { success: true });
    state = await WorkerPool.getWorker(worker.id);
    expect(state?.currentTasks).toBe(0);
    expect(state?.totalProcessed).toBe(1);
    expect(state?.totalFailed).toBe(0);
  });

  it('tracks failures on complete', async () => {
    const worker = await WorkerPool.registerWorker('w', ['noop']);
    await WorkerPool.assignTask(worker.id, buildTask('t1'));
    await WorkerPool.completeTask(worker.id, 't1', { success: false });

    const state = await WorkerPool.getWorker(worker.id);
    expect(state?.totalFailed).toBe(1);
  });
});

describe('WorkerPool stale detection', () => {
  it('marks workers offline when their heartbeat is older than the timeout', async () => {
    const worker = await WorkerPool.registerWorker('w', ['noop']);

    // Force an old heartbeat.
    const stored = JSON.parse(client.__strings.get(`worker:${worker.id}`)!);
    stored.lastHeartbeat = new Date(Date.now() - 120_000).toISOString();
    await client.set(`worker:${worker.id}`, JSON.stringify(stored));

    const staleCount = await WorkerPool.checkStaleWorkers(60);
    expect(staleCount).toBe(1);

    const state = await WorkerPool.getWorker(worker.id);
    expect(state?.status).toBe('offline');
  });

  it('does not mark fresh workers offline', async () => {
    await WorkerPool.registerWorker('w', ['noop']);
    expect(await WorkerPool.checkStaleWorkers(60)).toBe(0);
  });
});

describe('WorkerPool unregister', () => {
  it('removes the worker and its handler mapping', async () => {
    const worker = await WorkerPool.registerWorker('w', ['noop']);
    await WorkerPool.unregisterWorker(worker.id);

    expect(await WorkerPool.getWorker(worker.id)).toBeNull();
    expect(await client.sMembers('worker:handlers:map:noop')).not.toContain(worker.id);
  });
});
