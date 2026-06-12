# PR: Fix #38 — Implement Task Priorities with SLA

## Summary
The system required Priority-based SLAs (Service Level Agreements) to guarantee high-priority tasks are processed in a timely manner. Previously, a spike in low-priority tasks could occupy all worker slots and delay high-priority tasks beyond their SLA thresholds. This PR introduces a background SLA enforcement daemon within the `TaskScheduler` that detects when high-priority tasks are at risk of missing their SLA and proactively frees up worker capacity by preempting low-priority tasks.

## Root Cause
The system strictly followed priority scores in the queue, but `WorkerPool` had no concept of preemption. If a worker was actively processing a low-priority task (which can take minutes or hours), it was inaccessible to high-priority tasks sitting in the queue. 

```ts
// Previously, getNextTask() strictly obeyed the queue but
// couldn't forcefully interrupt running tasks on busy workers
const taskIds = await client.zRange(queueKey, 0, -1, { REV: true });
```

## Solution
1. **SLA Monitoring Loop:** Added `enforceSLA()` to the `TaskScheduler`'s background polling loop. It checks all `queued`, `pending`, and `retry` tasks to see if their age (`now - createdAt`) exceeds their defined `PRIORITY_SLA` (e.g., 15s for high, 5s for critical).
2. **Robust Runnability Checks:** Before marking a task as an SLA violator that warrants preemption, the system verifies the task is actually runnable (i.e. not blocked by dependencies, and not scheduled for the future). This prevents infinite preemption loops where a high-priority task repeatedly evicts low-priority tasks but is unable to execute itself.

```ts
// Ensure task is actually runnable
if (task.dependencies && task.dependencies.length > 0) {
  const depsResolved = await (TaskQueue as any)._checkDependencies(task.dependencies);
  if (!depsResolved) continue;
}
```

3. **Task Preemption:** If a high-priority task SLA is breached, running low- or medium-priority tasks are safely preempted. They are returned to the queue (status `'pending'`), and `WorkerPool.preemptTask()` immediately frees up the concurrency slot on the worker so it can poll for the high-priority task.
4. **Race Condition Prevention:** Modified `TaskExecutor` to check if a task was preempted (`workerId` changed or cleared) before applying the `completed` state, ensuring a preempted execution promise does not overwrite state or double-decrement worker capacity when it eventually finishes.
5. **Massive N+1 Performance Fix:** When hunting for tasks to preempt, the scheduler now iterates over active workers' assigned task lists via `WorkerPool` instead of doing a full scan across the historical `tasks:index`. This converts an unbounded $O(N)$ operation (where $N$ is all tasks ever created) into an extremely fast $O(W \times C)$ operation (Workers $\times$ Concurrency).
6. **Accurate Metrics (No Inflation):** SLA violations are tracked via a `slaViolated` metadata flag on the task. This ensures the SLA violation metric increments exactly once per task, preventing artificial inflation caused by the scheduler repeatedly scanning the same queued task every 5 seconds.

```ts
// Safely preempting tasks
if (t && t.status === 'processing' && (t.priority === 'low' || t.priority === 'medium')) {
  const previousWorkerId = t.workerId;
  await TaskQueue.updateTaskStatus(t.id, 'pending', { workerId: undefined, startedAt: undefined });
  
  if (previousWorkerId) {
    await WorkerPool.preemptTask(previousWorkerId, t.id); // Immediate capacity release
  }
}
```

## Testing

Added comprehensive coverage in `src/services/__tests__/task-scheduler.test.ts` and `src/services/task-executor.test.ts`:

| Test | What it covers |
|---|---|
| No SLA violation | Low-priority tasks continue executing when high-priority tasks are within SLA |
| SLA violation triggers preemption | Low-priority tasks are transitioned to `pending` and `slaViolations` are incremented |
| Blocked high-priority task | Verifies a high-priority task blocked by dependencies does not trigger preemption starvation loops |
| Future scheduled high-priority task | Verifies a high-priority task scheduled for the future does not trigger preemption starvation loops |
| Prevent duplicate completions | Verifies `TaskExecutor` checks if a task was preempted and avoids state corruption/capacity miscounts |

`npm test` passes. `npm run type-check` passes.

Fixes #38
