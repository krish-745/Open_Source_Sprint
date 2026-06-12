import { TaskQueue } from '../task-queue';
import { WorkerPool } from '../worker-pool';
import { TaskExecutor } from '../task-executor';
import * as redis from '../redis';

jest.mock('../redis');
const mockedGetRedisClient = redis.getRedisClient as jest.Mock;

function makeFakeRedis() {
  const strings = new Map<string, string>();
  const sets = new Map<string, Set<string>>();
  const zsets = new Map<string, Map<string, number>>();
  const lists = new Map<string, string[]>();
  const hashes = new Map<string, Map<string, string>>();

  const _hash = (k: string) => {
    if (!hashes.has(k)) hashes.set(k, new Map());
    return hashes.get(k)!;
  };

  return {
    set: jest.fn(async (k: string, v: string) => {
      strings.set(k, v);
      return 'OK';
    }),
    mGet: jest.fn(async (keys: string[]) => keys.map((k: string) => strings.get(k) ?? null)),
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
    expire: jest.fn(async () => 1),
    zAdd: jest.fn(async (k: string, { score, value }: { score: number; value: string }) => {
      const z = zsets.get(k) ?? new Map<string, number>();
      z.set(value, score);
      zsets.set(k, z);
      return 1;
    }),
    zCard: jest.fn(async (k: string) => {
      return zsets.get(k)?.size ?? 0;
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
    hSet: jest.fn(async (k: string, field: string, val: string) => {
      _hash(k).set(field, val);
      return 1;
    }),
    hGet: jest.fn(async (k: string, field: string) => _hash(k).get(field) ?? null),
    hGetAll: jest.fn(async (k: string) => Object.fromEntries(_hash(k))),
    hIncrBy: jest.fn(async (k: string, field: string, incr: number) => {
      const curr = parseInt(_hash(k).get(field) ?? '0', 10);
      const next = curr + incr;
      _hash(k).set(field, next.toString());
      return next;
    }),
    watch: jest.fn(async () => 'OK'),
    unwatch: jest.fn(async () => 'OK'),
    executeIsolated: jest.fn(async function(this: any, cb: any) { return cb(this); }),
    multi: jest.fn(() => {
      const operations: any[] = [];
      const m = {
        set: (k: string, v: string) => {
          operations.push(() => { strings.set(k, v); });
          return m;
        },
        zAdd: () => m,
        zRem: () => m,
        exec: async () => {
          for (const op of operations) op();
          return ['OK'];
        }
      };
      return m;
    }),
    __strings: strings,
  } as any;
}

let client: ReturnType<typeof makeFakeRedis>;

beforeEach(() => {
  client = makeFakeRedis();
  mockedGetRedisClient.mockReturnValue(client);
  TaskExecutor.clearHandlers();
});

describe('Task Consensus (Multi-Quorum)', () => {
  it('executes task on multiple workers and requires majority consensus', async () => {
    let callCount = 0;
    TaskExecutor.registerHandler('consensus-task', async () => {
      callCount++;
      if (callCount === 3) return 'B';
      return 'A';
    });

    const task = await TaskQueue.createTask('critical-task', 'consensus-task', {}, {
      consensus: {
        workers: 3,
        strategy: 'majority'
      }
    });

    const w1 = await WorkerPool.registerWorker('w1', ['consensus-task']);
    const w2 = await WorkerPool.registerWorker('w2', ['consensus-task']);
    const w3 = await WorkerPool.registerWorker('w3', ['consensus-task']);

    await WorkerPool.assignTask(w1.id, task);
    await WorkerPool.assignTask(w2.id, task);
    await WorkerPool.assignTask(w3.id, task);

    await TaskExecutor.execute(w1.id, task);
    let updatedTask = await TaskQueue.getTask(task.id);
    expect(updatedTask?.status).toBe('processing');

    await TaskExecutor.execute(w2.id, task);
    updatedTask = await TaskQueue.getTask(task.id);
    expect(updatedTask?.status).toBe('completed');
    expect(updatedTask?.result).toBe('A');

    await TaskExecutor.execute(w3.id, task);
    updatedTask = await TaskQueue.getTask(task.id);
    expect(updatedTask?.status).toBe('completed');
    expect(updatedTask?.result).toBe('A');
  });

  it('fails the task if consensus cannot be reached', async () => {
    let callCount = 0;
    TaskExecutor.registerHandler('strict-consensus-task', async () => {
      callCount++;
      if (callCount === 1) return 'A';
      if (callCount === 2) return 'B';
      return 'C';
    });

    const task = await TaskQueue.createTask('strict-task', 'strict-consensus-task', {}, {
      consensus: {
        workers: 3,
        strategy: 'all'
      }
    });

    const w1 = await WorkerPool.registerWorker('w1', ['strict-consensus-task']);
    const w2 = await WorkerPool.registerWorker('w2', ['strict-consensus-task']);
    const w3 = await WorkerPool.registerWorker('w3', ['strict-consensus-task']);

    await TaskExecutor.execute(w1.id, task);
    await TaskExecutor.execute(w2.id, task);
    await TaskExecutor.execute(w3.id, task);

    const updatedTask = await TaskQueue.getTask(task.id);
    expect(updatedTask?.status).toBe('failed');
    expect(updatedTask?.error).toMatch(/Consensus not reached/);
  });

  it('tolerates worker errors if majority reaches consensus', async () => {
    let callCount = 0;
    TaskExecutor.registerHandler('faulty-consensus-task', async () => {
      callCount++;
      if (callCount === 1) throw new Error('Worker crashed');
      return 'SUCCESS';
    });

    const task = await TaskQueue.createTask('faulty-task', 'faulty-consensus-task', {}, {
      consensus: {
        workers: 3,
        strategy: 'majority'
      }
    });

    const w1 = await WorkerPool.registerWorker('w1', ['faulty-consensus-task']);
    const w2 = await WorkerPool.registerWorker('w2', ['faulty-consensus-task']);
    const w3 = await WorkerPool.registerWorker('w3', ['faulty-consensus-task']);

    await TaskExecutor.execute(w1.id, task);
    await TaskExecutor.execute(w2.id, task);
    await TaskExecutor.execute(w3.id, task);

    const updatedTask = await TaskQueue.getTask(task.id);
    expect(updatedTask?.status).toBe('completed');
    expect(updatedTask?.result).toBe('SUCCESS');
  });

  it('fails the task if majority of workers throw errors', async () => {
    let callCount = 0;
    TaskExecutor.registerHandler('majority-faulty-task', async () => {
      callCount++;
      if (callCount <= 2) throw new Error('Network timeout');
      return 'SUCCESS';
    });

    const task = await TaskQueue.createTask('maj-faulty-task', 'majority-faulty-task', {}, {
      consensus: {
        workers: 3,
        strategy: 'majority'
      }
    });

    const w1 = await WorkerPool.registerWorker('w1', ['majority-faulty-task']);
    const w2 = await WorkerPool.registerWorker('w2', ['majority-faulty-task']);
    const w3 = await WorkerPool.registerWorker('w3', ['majority-faulty-task']);

    await TaskExecutor.execute(w1.id, task);
    await TaskExecutor.execute(w2.id, task);
    await TaskExecutor.execute(w3.id, task);

    const updatedTask = await TaskQueue.getTask(task.id);
    expect(updatedTask?.status).toBe('failed');
    expect(updatedTask?.error).toMatch(/Network timeout/);
  });
  it('handles weighted consensus with heavy workers', async () => {
    let callCount = 0;
    TaskExecutor.registerHandler('weighted-consensus', async () => {
      callCount++;
      if (callCount === 1) return 'A'; // Heavy worker
      return 'B'; // Light workers
    });

    const task = await TaskQueue.createTask('weighted-task', 'weighted-consensus', {}, {
      consensus: { workers: 3, strategy: 'weighted' }
    });

    // w1 has weight 5, w2 and w3 have weight 1
    const w1 = await WorkerPool.registerWorker('w1', ['weighted-consensus'], { tags: ['weight:5'] });
    const w2 = await WorkerPool.registerWorker('w2', ['weighted-consensus'], { tags: ['weight:1'] });
    const w3 = await WorkerPool.registerWorker('w3', ['weighted-consensus'], { tags: ['weight:abc'] }); // invalid weight gracefully falls back to 1

    await TaskExecutor.execute(w1.id, task);
    await TaskExecutor.execute(w2.id, task);
    await TaskExecutor.execute(w3.id, task);

    // w1 returns 'A' (weight 5). w2 and w3 return 'B' (weight 1 each). Total weight = 7. Majority > 3.5.
    // 'A' has 5 weight, 'B' has 2 weight. 'A' should win!
    const updatedTask = await TaskQueue.getTask(task.id);
    expect(updatedTask?.status).toBe('completed');
    expect(updatedTask?.result).toBe('A');
  });

  it('fails safely if totalSubmitted > totalExpected and no majority exists', async () => {
    let callCount = 0;
    TaskExecutor.registerHandler('overflow-consensus', async () => {
      callCount++;
      return `RESULT_${callCount}`; // Every worker returns something different
    });

    const task = await TaskQueue.createTask('overflow-task', 'overflow-consensus', {}, {
      consensus: { workers: 2, strategy: 'majority' }
    });

    const w1 = await WorkerPool.registerWorker('w1', ['overflow-consensus']);
    const w2 = await WorkerPool.registerWorker('w2', ['overflow-consensus']);
    const w3 = await WorkerPool.registerWorker('w3', ['overflow-consensus']);

    await TaskExecutor.execute(w1.id, task);
    await TaskExecutor.execute(w2.id, task);
    
    // 2 workers ran, neither agree (RESULT_1, RESULT_2). Task should be failed!
    let updatedTask = await TaskQueue.getTask(task.id);
    expect(updatedTask?.status).toBe('failed');

    // 3rd worker (straggler) arrives. The totalSubmitted is now 3 > 2 (expected).
    // It should gracefully exit as straggler.
    await TaskExecutor.execute(w3.id, task);
    updatedTask = await TaskQueue.getTask(task.id);
    expect(updatedTask?.status).toBe('failed');
  });
});
