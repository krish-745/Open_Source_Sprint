# PR: Fix #40 — Implement Task Consensus (Multi-Quorum)

## Summary

Critical tasks had no way to guard against a single rogue or faulty worker producing a wrong result. `TaskExecutor.execute()` treated every worker result as authoritative the moment it arrived, with no mechanism to cross-check it against other workers. This PR introduces full multi-quorum consensus: a task can now be dispatched to N workers simultaneously, and its final outcome is decided only when a configurable agreement threshold is met.

## Root Cause

The executor committed the task result as soon as the first worker finished:

```ts
// ❌ Before: first worker to finish wins unconditionally
const result = await Promise.race([handler(task.payload), timeout]);
await TaskQueue.updateTaskStatus(task.id, 'completed', { result });
```

There was no type-level field to attach quorum requirements to a task, no distributed state to accumulate results from multiple workers, and no logic to defer finalisation until a threshold was satisfied. A single byzantine or misconfigured worker could silently corrupt the final result of a mission-critical task.

## Solution

### `src/types/index.ts`

- **`ConsensusOptions`** — new exported interface: `{ workers: number; strategy: 'majority' | 'all' | 'weighted' }`.
- **`Task`** — new optional field `consensus?: ConsensusOptions`. Fully backward-compatible; tasks without the field behave exactly as before.

### `src/services/task-queue.ts`

- **`createTask`** — accepts `consensus` in its options bag and persists it on the stored `Task` object so the executor can read it back.
- **`createTasksBatch`** — forwards `consensus` through its `options` passthrough, keeping batch creation consistent with single-task creation.

### `src/services/task-executor.ts`

The execute method was surgically restructured. All changes are gated on `task.consensus && task.consensus.workers > 1`; the non-consensus code path is fully preserved.

**Handler execution is now fault-isolated.** The handler runs inside its own inner `try/catch` rather than the outer `try`, so a worker error is captured as a typed value rather than immediately unwinding the stack:

```ts
// ✅ After: errors are a first-class result, not an exception
let executionResult: any;
let executionError: Error | undefined;
try {
  executionResult = await Promise.race([handler(task.payload), timeout]);
} catch (err: any) {
  executionError = err;
}
```

This is essential for consensus: a worker that throws must still cast a vote (as an error-valued entry) rather than bypassing the quorum machinery entirely.

**Consensus state lives in a Redis hash** (`task:<id>:consensus`). Each worker writes its result (or error) as a JSON envelope:

```ts
// success:  { __consensus_result: <value> }
// failure:  { __consensus_error:  <message> }
```

The `__consensus_*` namespace prevents ambiguity with legitimate `null` or `false` results. After writing, the executor reads all submitted entries and tallies them.

**Three strategies are supported:**

| Strategy | Completes when | Fails when |
|---|---|---|
| `majority` | A single value appears in ≥ ⌊N/2⌋+1 entries | All N submitted and no majority exists |
| `all` | All N entries are identical | All N submitted and any entry differs |
| `weighted` | A value's weight-sum exceeds half the total weight | All N submitted and no value clears that threshold |

Weights are read from the worker's `tags` array (`weight:<N>`), so they require no interface changes to `Worker`.

**Straggler safety.** Once a task reaches `completed` or `failed`, any late worker that arrives finds the task already settled and exits immediately — but only after calling `WorkerPool.completeTask` to correctly decrement `currentTasks`:

```ts
// ✅ completeTask called before return so currentTasks never leaks
if (currentTask.status === 'completed' || currentTask.status === 'failed') {
  await WorkerPool.completeTask(workerId, task.id, { ... });
  return;
}
```

Without this, a straggler would return early and leave its `currentTasks` counter permanently incremented, eventually pinning the worker in the `busy` state even after it had finished all real work.

**Outer crash handling is restored.** The outer `catch` (which handles pre-execution failures such as an unregistered handler) now calls `TaskQueue.retryTask` before marking the task `failed`, matching the behaviour that existed before the refactor.

### `src/routes/api.ts`

- **`POST /tasks`** — destructures and forwards `consensus` from the request body.
- **`POST /tasks/batch`** — maps `t.consensus` into each task's options object.
- **Input validation** — both routes reject malformed consensus up-front: unknown `strategy` values return 400, and `workers < 2` or non-integer values return 400. This prevents silent misbehaviour from reaching Redis.

```ts
if (consensus !== undefined) {
  const validStrategies = ['majority', 'all', 'weighted'];
  if (!validStrategies.includes(consensus.strategy))
    return res.status(400).json({ error: `Invalid consensus.strategy...` });
  if (!Number.isInteger(consensus.workers) || consensus.workers < 2)
    return res.status(400).json({ error: 'consensus.workers must be an integer >= 2' });
}
```

## Testing

Added `custom_tests/issue40/regression.test.ts` and `src/services/__tests__/task-consensus.test.ts` (Redis mocked via `makeFakeRedis`):

| Test | What it covers |
|---|---|
| Majority consensus reached | 3 workers, 2 agree on `'A'`, 1 returns `'B'` — task completes after the 2nd worker finishes, not the 1st |
| Task holds `processing` until quorum | After 1st worker only, status is still `processing` |
| `all` strategy fails on disagreement | 3 workers return `'A'`, `'B'`, `'C'` — task fails with `Consensus not reached` |
| Worker error tolerated by majority | Worker 1 throws; workers 2 & 3 return `'SUCCESS'` — task completes with `'SUCCESS'` |
| Majority of errors → task fails | Workers 1 & 2 throw `'Network timeout'`; worker 3 succeeds — task fails propagating the error message |
| Straggler does not re-execute handler | After majority completes the task, 3rd worker detects settled state, skips handler, calls `completeTask` — `callCount` stays at 2, not 3 |
| Weighted consensus | `weight:5` heavy worker overrules two `weight:1` light workers when strategies disagree |
| Overflow overflow (≥ expected workers) | Safely handles edge cases where more workers submit results than the quorum strictly expected without deadlocking |

Also updated `src/services/task-queue.batch.test.ts` to add `zCard` and `hGet` to its hand-rolled Redis mock, as `createTask` now calls `zCard` for queue-size enforcement on every invocation.

`npm test` — 42 passed, 0 failed. `npm run type-check` — no errors.

## Notes

Consensus tasks are intentionally not retried at the task level after a full quorum evaluation fails. Re-running the same N workers against a stale Redis hash would produce a corrupted tally (old votes + new votes for the same worker IDs). The correct pattern is for the caller to submit a fresh task. This is documented in code and is a deliberate design constraint, not an omission.

Fixes #40
