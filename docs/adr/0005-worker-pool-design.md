# 5. Worker pool design

Status: Accepted

## Context
Tasks must be dispatched to workers that can run the required handler and have spare capacity, and the system must detect workers that have died.

## Decision
`WorkerPool` models workers as Redis records with explicit lifecycle and capacity:

- Registration stores a worker record and maps each declared handler to the worker via a set (`worker:handlers:map:<handler>`).
- Capacity is tracked as `currentTasks / maxConcurrent`; `assignTask`/`completeTask` keep it current.
- `getAvailableWorkers(handler)` returns workers for that handler with free capacity, sorted least-busy first.
- Liveness is tracked via `lastHeartbeat`; `checkStaleWorkers(timeout)` marks workers offline when heartbeats stop.

## Consequences
- Pros: handler-aware, capacity-aware dispatch; stale detection prevents routing to dead workers.
- Cons: worker status transitions are driven by callers (executor/scheduler), so status must be derived from actual load — e.g. a worker is only idle when it has no remaining tasks, not simply because one task finished.
- Heartbeat and stale-detection intervals must be tuned together to avoid false offline marking.
