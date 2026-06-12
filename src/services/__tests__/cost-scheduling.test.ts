/**
 * Tests for Issue #42: Cost-Based Scheduling
 *
 * Covers:
 *   - TaskQueue: BudgetExceededError, setCallerBudget, getCallerBudget,
 *     budget-aware createTask, predictQueueCost
 *   - WorkerPool: setWorkerCost, getWorkerCost, getWorkerCostMetrics,
 *     cost-sorted getAvailableWorkers, cost accrual in completeTask
 *
 * Design: zero changes to Task/Worker interfaces. Cost lives in
 * task.metadata.cost / task.metadata.callerId and in separate Redis
 * hashes (caller:budgets, worker:cost:config, worker:cost:total).
 */

import { TaskQueue, BudgetExceededError } from '../task-queue';
import { WorkerPool } from '../worker-pool';
import * as redis from '../redis';
import { Task } from '../../types';

jest.mock('../redis');
const mockedGetRedisClient = redis.getRedisClient as jest.Mock;

// ---------------------------------------------------------------------------
// Shared fake-Redis factory — includes every command used by both services.
// ---------------------------------------------------------------------------
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
    // --- string ---
    set: jest.fn(async (k: string, v: string) => { strings.set(k, v); return 'OK'; }),
    get: jest.fn(async (k: string) => strings.get(k) ?? null),
    del: jest.fn(async (k: string) => { strings.delete(k); lists.delete(k); return 1; }),

    // --- set ---
    sAdd: jest.fn(async (k: string, m: string) => {
      const s = sets.get(k) ?? new Set<string>();
      s.add(m); sets.set(k, s); return 1;
    }),
    sMembers: jest.fn(async (k: string) => Array.from(sets.get(k) ?? [])),
    sRem: jest.fn(async (k: string, m: string) => { sets.get(k)?.delete(m); return 1; }),

    // --- sorted set ---
    zAdd: jest.fn(async (k: string, { score, value }: { score: number; value: string }) => {
      const z = zsets.get(k) ?? new Map<string, number>();
      z.set(value, score); zsets.set(k, z); return 1;
    }),
    zRange: jest.fn(async (k: string, start: number, stop: number, _opts?: any) => {
      const z = zsets.get(k);
      if (!z) return [];
      const ordered = [...z.entries()].sort((a, b) => a[1] - b[1]).map(e => e[0]);
      return stop === -1 ? ordered.slice(start) : ordered.slice(start, stop + 1);
    }),
    zCard: jest.fn(async (k: string) => zsets.get(k)?.size ?? 0),
    zRem: jest.fn(async (k: string, m: string) => { zsets.get(k)?.delete(m); return 1; }),

    // --- list ---
    lPush: jest.fn(async (k: string, v: string) => {
      const l = lists.get(k) ?? []; l.unshift(v); lists.set(k, l); return l.length;
    }),
    lRem: jest.fn(async (k: string, _count: number, v: string) => {
      const l = lists.get(k) ?? [];
      const i = l.indexOf(v);
      if (i >= 0) l.splice(i, 1);
      return 1;
    }),

    // --- hash ---
    hSet: jest.fn(async (k: string, field: string, val: string) => {
      _hash(k).set(field, val); return 1;
    }),
    hGet: jest.fn(async (k: string, field: string) => _hash(k).get(field) ?? null),
    hGetAll: jest.fn(async (k: string) => Object.fromEntries(_hash(k))),
    hIncrBy: jest.fn(async (k: string, field: string, incr: number) => {
      const curr = parseInt(_hash(k).get(field) ?? '0', 10);
      const next = curr + incr;
      _hash(k).set(field, next.toString());
      return next;
    }),

    // --- pattern ---
    keys: jest.fn(async () => [] as string[]),

    // --- transactions / batch ---
    mGet: jest.fn(async (keys: string[]) => keys.map((k: string) => strings.get(k) ?? null)),
    hDel: jest.fn(async (k: string, field: string) => { _hash(k).delete(field); return 1; }),
    multi: jest.fn(() => {
      const m: any = {
        zRem: jest.fn((k, val) => { zsets.get(k)?.delete(val); return m; }),
        zAdd: jest.fn((k, item) => {
          const z = zsets.get(k) ?? new Map<string, number>();
          z.set(item.value, item.score); zsets.set(k, z); return m;
        }),
        exec: jest.fn(async () => [['OK']]),
      };
      return m;
    }),

    __strings: strings,
    __hashes: hashes,
  } as any;
}

function buildTask(overrides: Partial<Task> = {}): Task {
  return {
    id: 'task-1',
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
    ...overrides,
  };
}

let client: ReturnType<typeof makeFakeRedis>;

beforeEach(() => {
  client = makeFakeRedis();
  mockedGetRedisClient.mockReturnValue(client);
});

// ===========================================================================
// BudgetExceededError class
// ===========================================================================
describe('BudgetExceededError', () => {
  it('is an instance of Error', () => {
    const err = new BudgetExceededError('bob', 5, 10);
    expect(err).toBeInstanceOf(Error);
    expect(err).toBeInstanceOf(BudgetExceededError);
  });

  it('message mentions the caller, available and required values', () => {
    const err = new BudgetExceededError('alice', 3, 50);
    expect(err.message).toMatch(/alice/);
    expect(err.message).toMatch(/budget exceeded/i);
  });

  it('name is BudgetExceededError', () => {
    expect(new BudgetExceededError('x', 0, 1).name).toBe('BudgetExceededError');
  });
});

// ===========================================================================
// Caller budget management
// ===========================================================================
describe('TaskQueue — caller budget management', () => {
  it('setCallerBudget stores and getCallerBudget retrieves the budget', async () => {
    await TaskQueue.setCallerBudget('alice', 100);
    expect(await TaskQueue.getCallerBudget('alice')).toBe(100);
  });

  it('getCallerBudget returns null when no budget has been set', async () => {
    expect(await TaskQueue.getCallerBudget('nobody')).toBeNull();
  });

  it('setCallerBudget overwrites a previous budget', async () => {
    await TaskQueue.setCallerBudget('bob', 50);
    await TaskQueue.setCallerBudget('bob', 200);
    expect(await TaskQueue.getCallerBudget('bob')).toBe(200);
  });

  it('setCallerBudget supports a budget of zero', async () => {
    await TaskQueue.setCallerBudget('carol', 0);
    expect(await TaskQueue.getCallerBudget('carol')).toBe(0);
  });
});

// ===========================================================================
// Budget enforcement in createTask
// ===========================================================================
describe('TaskQueue — budget-aware createTask', () => {
  it('throws BudgetExceededError when task cost exceeds available budget', async () => {
    await TaskQueue.setCallerBudget('alice', 40);
    await expect(
      TaskQueue.createTask('t', 'h', {}, { metadata: { cost: 50, callerId: 'alice' } })
    ).rejects.toBeInstanceOf(BudgetExceededError);
  });

  it('throws when budget is exactly 0 and cost > 0', async () => {
    await TaskQueue.setCallerBudget('zero', 0);
    await expect(
      TaskQueue.createTask('t', 'h', {}, { metadata: { cost: 1, callerId: 'zero' } })
    ).rejects.toBeInstanceOf(BudgetExceededError);
  });

  it('allows task creation when cost equals the remaining budget (boundary)', async () => {
    await TaskQueue.setCallerBudget('exact', 10);
    const task = await TaskQueue.createTask('t', 'h', {}, { metadata: { cost: 10, callerId: 'exact' } });
    expect(task).toBeDefined();
    expect(task.metadata.cost).toBe(10);
  });

  it('deducts the task cost from the caller budget after creation', async () => {
    await TaskQueue.setCallerBudget('dan', 100);
    await TaskQueue.createTask('t', 'h', {}, { metadata: { cost: 30, callerId: 'dan' } });
    expect(await TaskQueue.getCallerBudget('dan')).toBe(70);
  });

  it('deducts sequentially for multiple tasks by the same caller', async () => {
    await TaskQueue.setCallerBudget('eve', 100);
    await TaskQueue.createTask('t1', 'h', {}, { metadata: { cost: 40, callerId: 'eve' } });
    await TaskQueue.createTask('t2', 'h', {}, { metadata: { cost: 40, callerId: 'eve' } });
    expect(await TaskQueue.getCallerBudget('eve')).toBe(20);

    // A third task that exceeds remaining budget must be rejected.
    await expect(
      TaskQueue.createTask('t3', 'h', {}, { metadata: { cost: 30, callerId: 'eve' } })
    ).rejects.toBeInstanceOf(BudgetExceededError);
  });

  it('allows task creation with callerId but no pre-set budget (opt-in semantics)', async () => {
    // No setCallerBudget call for 'frank' — getCallerBudget returns null.
    const task = await TaskQueue.createTask('t', 'h', {}, { metadata: { cost: 999, callerId: 'frank' } });
    expect(task).toBeDefined();
    // No deduction should occur when budget is null.
    expect(await TaskQueue.getCallerBudget('frank')).toBeNull();
  });

  it('allows task creation when no callerId is provided (no budget enforcement)', async () => {
    const task = await TaskQueue.createTask('t', 'h', {}, {});
    expect(task).toBeDefined();
  });

  it('treats an explicit cost of 0 as a free task (not default cost of 1)', async () => {
    await TaskQueue.setCallerBudget('gale', 0);
    // cost=0 means free — should NOT throw, even with budget=0.
    const task = await TaskQueue.createTask('free', 'h', {}, { metadata: { cost: 0, callerId: 'gale' } });
    expect(task).toBeDefined();
    // Budget remains 0 (no deduction for free tasks).
    expect(await TaskQueue.getCallerBudget('gale')).toBe(0);
  });

  it('normalises a negative cost to 1 to prevent budget inflation', async () => {
    await TaskQueue.setCallerBudget('neg', 5);
    // cost=-99 normalised to 1; budget 5 >= 1 so task is allowed and deducts 1.
    const task = await TaskQueue.createTask('t', 'h', {}, { metadata: { cost: -99, callerId: 'neg' } });
    expect(task).toBeDefined();
    expect(await TaskQueue.getCallerBudget('neg')).toBe(4);
  });

  it('normalises a NaN cost to 1 to prevent corrupted budget arithmetic', async () => {
    await TaskQueue.setCallerBudget('nan', 5);
    const task = await TaskQueue.createTask('t', 'h', {}, { metadata: { cost: NaN, callerId: 'nan' } });
    expect(task).toBeDefined();
    expect(await TaskQueue.getCallerBudget('nan')).toBe(4);
  });

  it('defaults cost to 1 when metadata.cost is absent', async () => {
    await TaskQueue.setCallerBudget('hank', 1);
    // cost defaults to 1, so creation succeeds and deducts 1.
    await TaskQueue.createTask('t', 'h', {}, { metadata: { callerId: 'hank' } });
    expect(await TaskQueue.getCallerBudget('hank')).toBe(0);
  });

  it('task object carries cost in metadata after creation', async () => {
    const task = await TaskQueue.createTask('t', 'h', {}, { metadata: { cost: 7 } });
    expect(task.metadata.cost).toBe(7);
  });
});

// ===========================================================================
// predictQueueCost
// ===========================================================================
describe('TaskQueue — predictQueueCost', () => {
  it('returns totalCost=0 and taskCount=0 for an empty queue', async () => {
    const result = await TaskQueue.predictQueueCost('empty-q');
    expect(result).toEqual({ totalCost: 0, taskCount: 0 });
  });

  it('sums costs across multiple tasks', async () => {
    await TaskQueue.createTask('t1', 'h', {}, { queueName: 'q1', metadata: { cost: 10 } });
    await TaskQueue.createTask('t2', 'h', {}, { queueName: 'q1', metadata: { cost: 15 } });
    const result = await TaskQueue.predictQueueCost('q1');
    expect(result.totalCost).toBe(25);
    expect(result.taskCount).toBe(2);
  });

  it('counts tasks with no explicit cost as 1 each', async () => {
    await TaskQueue.createTask('t1', 'h', {}, { queueName: 'q2' });
    await TaskQueue.createTask('t2', 'h', {}, { queueName: 'q2' });
    const result = await TaskQueue.predictQueueCost('q2');
    expect(result.totalCost).toBe(2); // 1 + 1
    expect(result.taskCount).toBe(2);
  });

  it('counts tasks with explicit cost=0 as 0 (free tasks are not inflated to 1)', async () => {
    await TaskQueue.createTask('t1', 'h', {}, { queueName: 'q3', metadata: { cost: 0 } });
    await TaskQueue.createTask('t2', 'h', {}, { queueName: 'q3', metadata: { cost: 5 } });
    const result = await TaskQueue.predictQueueCost('q3');
    expect(result.totalCost).toBe(5); // 0 + 5
    expect(result.taskCount).toBe(2);
  });

  it('handles mixed cost and no-cost tasks', async () => {
    await TaskQueue.createTask('t1', 'h', {}, { queueName: 'q4', metadata: { cost: 20 } });
    await TaskQueue.createTask('t2', 'h', {}, { queueName: 'q4' }); // cost defaults to 1
    await TaskQueue.createTask('t3', 'h', {}, { queueName: 'q4', metadata: { cost: 5 } });
    const result = await TaskQueue.predictQueueCost('q4');
    expect(result.totalCost).toBe(26); // 20 + 1 + 5
    expect(result.taskCount).toBe(3);
  });
});

// ===========================================================================
// Worker cost configuration
// ===========================================================================
describe('WorkerPool — worker cost configuration', () => {
  it('getWorkerCost returns 1 (default) when no cost has been configured', async () => {
    const w = await WorkerPool.registerWorker('w', ['h']);
    expect(await WorkerPool.getWorkerCost(w.id)).toBe(1);
  });

  it('setWorkerCost persists and getWorkerCost retrieves the configured value', async () => {
    const w = await WorkerPool.registerWorker('w', ['h']);
    await WorkerPool.setWorkerCost(w.id, 15);
    expect(await WorkerPool.getWorkerCost(w.id)).toBe(15);
  });

  it('setWorkerCost overwrites a previously configured cost', async () => {
    const w = await WorkerPool.registerWorker('w', ['h']);
    await WorkerPool.setWorkerCost(w.id, 10);
    await WorkerPool.setWorkerCost(w.id, 25);
    expect(await WorkerPool.getWorkerCost(w.id)).toBe(25);
  });

  it('getWorkerCost supports a cost of 0', async () => {
    const w = await WorkerPool.registerWorker('w', ['h']);
    await WorkerPool.setWorkerCost(w.id, 0);
    expect(await WorkerPool.getWorkerCost(w.id)).toBe(0);
  });
});

// ===========================================================================
// Cost-sorted getAvailableWorkers
// ===========================================================================
describe('WorkerPool — cost-sorted getAvailableWorkers', () => {
  it('returns cheapest worker first', async () => {
    const wExpensive = await WorkerPool.registerWorker('expensive', ['svc']);
    const wCheap = await WorkerPool.registerWorker('cheap', ['svc']);
    await WorkerPool.setWorkerCost(wExpensive.id, 20);
    await WorkerPool.setWorkerCost(wCheap.id, 5);

    const workers = await WorkerPool.getAvailableWorkers('svc');
    expect(workers[0].id).toBe(wCheap.id);
    expect(workers[1].id).toBe(wExpensive.id);
  });

  it('breaks cost ties by capacity (least busy first)', async () => {
    const w1 = await WorkerPool.registerWorker('w1', ['svc'], { maxConcurrent: 2 });
    const w2 = await WorkerPool.registerWorker('w2', ['svc'], { maxConcurrent: 2 });
    // Both cost 10; w1 gets one task so it is busier.
    await WorkerPool.setWorkerCost(w1.id, 10);
    await WorkerPool.setWorkerCost(w2.id, 10);
    await WorkerPool.assignTask(w1.id, buildTask({ id: 'tx', handler: 'svc' }));

    const workers = await WorkerPool.getAvailableWorkers('svc');
    // w2 is less busy, so it should come first.
    expect(workers[0].id).toBe(w2.id);
  });

  it('returns workers ordered cheapest-to-most-expensive across three tiers', async () => {
    const w1 = await WorkerPool.registerWorker('tier-high', ['job']);
    const w2 = await WorkerPool.registerWorker('tier-mid', ['job']);
    const w3 = await WorkerPool.registerWorker('tier-low', ['job']);
    await WorkerPool.setWorkerCost(w1.id, 100);
    await WorkerPool.setWorkerCost(w2.id, 50);
    await WorkerPool.setWorkerCost(w3.id, 10);

    const workers = await WorkerPool.getAvailableWorkers('job');
    expect(workers.map(w => w.id)).toEqual([w3.id, w2.id, w1.id]);
  });

  it('excludes offline workers even if they have lowest cost', async () => {
    const wOnline = await WorkerPool.registerWorker('online', ['svc2']);
    const wOffline = await WorkerPool.registerWorker('offline', ['svc2']);
    await WorkerPool.setWorkerCost(wOnline.id, 20);
    await WorkerPool.setWorkerCost(wOffline.id, 1); // cheapest but offline
    await WorkerPool.updateWorkerStatus(wOffline.id, 'offline');

    const workers = await WorkerPool.getAvailableWorkers('svc2');
    const ids = workers.map(w => w.id);
    expect(ids).toContain(wOnline.id);
    expect(ids).not.toContain(wOffline.id);
  });
});

// ===========================================================================
// Worker cost accrual in completeTask / getWorkerCostMetrics
// ===========================================================================
describe('WorkerPool — cost accrual and metrics', () => {
  it('getWorkerCostMetrics returns zero totalCostAccrued before any task completes', async () => {
    const w = await WorkerPool.registerWorker('w', ['h']);
    await WorkerPool.setWorkerCost(w.id, 5);
    const metrics = await WorkerPool.getWorkerCostMetrics(w.id);
    expect(metrics.costPerTask).toBe(5);
    expect(metrics.totalCostAccrued).toBe(0);
  });

  it('accrues task.metadata.cost when task carries an explicit cost', async () => {
    const w = await WorkerPool.registerWorker('w', ['h']);
    const task = buildTask({ id: 'task-cost', metadata: { cost: 12 } });
    // Persist task so getTask can retrieve it inside completeTask.
    await client.set('task:task-cost', JSON.stringify(task));
    await WorkerPool.assignTask(w.id, task);
    await WorkerPool.completeTask(w.id, task.id, { success: true });
    const metrics = await WorkerPool.getWorkerCostMetrics(w.id);
    expect(metrics.totalCostAccrued).toBe(12);
  });

  it('falls back to workerCostPerTask when task has no metadata.cost', async () => {
    const w = await WorkerPool.registerWorker('w', ['h']);
    await WorkerPool.setWorkerCost(w.id, 7);
    const task = buildTask({ id: 'task-no-cost', metadata: {} });
    await client.set('task:task-no-cost', JSON.stringify(task));
    await WorkerPool.assignTask(w.id, task);
    await WorkerPool.completeTask(w.id, task.id, { success: true });
    const metrics = await WorkerPool.getWorkerCostMetrics(w.id);
    expect(metrics.totalCostAccrued).toBe(7);
  });

  it('accumulates costs across multiple completed tasks', async () => {
    const w = await WorkerPool.registerWorker('w', ['h']);

    for (let i = 0; i < 3; i++) {
      const task = buildTask({ id: `task-acc-${i}`, metadata: { cost: 10 } });
      await client.set(`task:task-acc-${i}`, JSON.stringify(task));
      await WorkerPool.assignTask(w.id, task);
      await WorkerPool.completeTask(w.id, task.id, { success: true });
    }

    const metrics = await WorkerPool.getWorkerCostMetrics(w.id);
    expect(metrics.totalCostAccrued).toBe(30);
  });

  it('accrues correctly for failed tasks too (cost is incurred regardless)', async () => {
    const w = await WorkerPool.registerWorker('w', ['h']);
    const task = buildTask({ id: 'task-fail', metadata: { cost: 8 } });
    await client.set('task:task-fail', JSON.stringify(task));
    await WorkerPool.assignTask(w.id, task);
    await WorkerPool.completeTask(w.id, task.id, { success: false });
    const metrics = await WorkerPool.getWorkerCostMetrics(w.id);
    expect(metrics.totalCostAccrued).toBe(8);
  });

  it('accrues 0 for tasks with explicit cost=0', async () => {
    const w = await WorkerPool.registerWorker('w', ['h']);
    await WorkerPool.setWorkerCost(w.id, 5); // worker default cost = 5
    const task = buildTask({ id: 'task-free', metadata: { cost: 0 } });
    await client.set('task:task-free', JSON.stringify(task));
    await WorkerPool.assignTask(w.id, task);
    await WorkerPool.completeTask(w.id, task.id, { success: true });
    const metrics = await WorkerPool.getWorkerCostMetrics(w.id);
    // task.metadata.cost is 0 (explicit free), so accrued should be 0 not 5.
    expect(metrics.totalCostAccrued).toBe(0);
  });

  it('gracefully falls back to workerCostPerTask when task no longer exists in Redis', async () => {
    const w = await WorkerPool.registerWorker('w', ['h']);
    await WorkerPool.setWorkerCost(w.id, 3);
    const task = buildTask({ id: 'task-gone' });
    // Do NOT persist the task, simulating a task that was cleaned up before completeTask runs.
    await WorkerPool.assignTask(w.id, task);
    await WorkerPool.completeTask(w.id, task.id, { success: true });
    const metrics = await WorkerPool.getWorkerCostMetrics(w.id);
    expect(metrics.totalCostAccrued).toBe(3);
  });
});
