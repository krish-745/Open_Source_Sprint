# PR: Fix #42 — Implement Cost-Based Scheduling

## Summary

Tasks had no way to declare a resource cost, workers had no cost configuration, and the scheduler had no mechanism to prefer cheaper workers or enforce caller spending limits. This PR implements cost-based scheduling entirely through existing extension points — no changes to the `Task` or `Worker` interfaces.

## Root Cause

The scheduler's `getAvailableWorkers()` sorted workers by capacity alone, with no awareness of cost. There was no budget concept per caller, no per-worker cost configuration, and no way to predict the cost of a pending queue.

## Solution

Cost data lives in two places that already existed:

- **`task.metadata.cost`** — per-task cost units (any non-negative number; defaults to `1`)
- **`task.metadata.callerId`** — identifies the caller for budget enforcement

Worker cost config and totals live in dedicated Redis hashes that are fully independent of the `Worker` object, so no existing interface is touched:

| Redis key | Purpose |
|---|---|
| `caller:budgets` | `callerId → remaining budget` |
| `worker:cost:config` | `workerId → costPerTask` |
| `worker:cost:total` | `workerId → lifetime cost accrued` |

### `src/services/task-queue.ts`

- **`BudgetExceededError`** — new exported error class, same pattern as the existing `QueueFullError`
- **`setCallerBudget(callerId, budget)`** — writes to `caller:budgets` hash
- **`getCallerBudget(callerId)`** — reads from `caller:budgets` hash; returns `null` when no budget is set (opt-in enforcement)
- **`createTask`** — reads `metadata.cost` and `metadata.callerId`; if a budget exists and is insufficient, throws `BudgetExceededError`; deducts cost after the task is fully persisted to prevent silent budget drain on write failure
- **`predictQueueCost(queueName)`** — scans the queue sorted set, sums `metadata.cost` per task, returns `{ totalCost, taskCount }`

Cost values are normalised: `?? 1` (not `|| 1`) honours explicit `cost: 0` as a free task; negative or non-finite values are clamped to `1` to prevent budget corruption.

### `src/services/worker-pool.ts`

- **`setWorkerCost(workerId, costPerTask)`** — writes to `worker:cost:config` hash
- **`getWorkerCost(workerId)`** — reads from hash; defaults to `1` when not configured
- **`getAvailableWorkers`** — after collecting online workers, fetches each worker's `costPerTask` and sorts by cost ascending, breaking ties by capacity (least busy first). Cheapest eligible worker always comes first.
- **`completeTask`** — after the task finishes, atomically increments `worker:cost:total[workerId]` by the task's cost (`task.metadata.cost ?? workerDefault`; same `??` + clamp logic)
- **`getWorkerCostMetrics(workerId)`** — returns `{ costPerTask, totalCostAccrued }` from the two hashes

### `src/services/metrics-collector.ts`

- **`captureSnapshot`** — merges `getWorkerCostMetrics` into each worker's metrics entry and attaches `predictQueueCost` output to each queue entry. No structural changes to `SystemMetrics`; cost data flows into the already-untyped `Record<string, any>` fields.

## Testing

Added `src/services/__tests__/cost-scheduling.test.ts` (51 tests, Redis mocked via shared `makeFakeRedis`):

| Group | What it covers |
|---|---|
| `BudgetExceededError` | instanceof, message content, name |
| Caller budget management | set/get, overwrite, zero budget |
| Budget-aware `createTask` | exceeds budget, zero budget, exact boundary, deduction after persist, sequential deduction, no-budget opt-in, no callerId, free task (`cost: 0`), negative cost normalised to 1, NaN normalised to 1, absent cost defaults to 1 |
| `predictQueueCost` | empty queue, sum of explicit costs, default cost, zero-cost tasks, mixed |
| Worker cost config | default 1, set/get, overwrite, zero cost |
| Cost-sorted `getAvailableWorkers` | cheapest first, tie-break by capacity, three-tier ordering, offline excluded despite lowest cost |
| Cost accrual + `getWorkerCostMetrics` | zero before completion, explicit cost, fallback to worker default, accumulation across tasks, failed tasks still accrue, free task accrues 0, missing task graceful fallback |

Also updated `src/services/__tests__/worker-pool.test.ts` and `src/services/task-queue.batch.test.ts` to add `hSet`/`hGet` to their hand-rolled Redis mocks, as our new code paths call these commands.

`npm test` — 75 passed. `npm run type-check` — no errors.

## Notes

The issue description mentions "minimize total cost **while meeting deadlines**." The six acceptance criteria do not include deadline-aware scheduling, and the `Task` interface has no `deadline` field — only `scheduledFor` (a "not before" guard, not a "must finish by" constraint). Adding a true deadline field would require modifying `src/types/index.ts`, which would break every other contributor's open PR.

This PR satisfies all six listed acceptance criteria. Deadline-aware cost trade-offs (e.g. preferring a more expensive but available worker when a deadline is imminent) are a natural follow-on and can be implemented non-invasively via `task.metadata.deadline` in a future issue, using the same zero-interface-change pattern established here.

Fixes #42
