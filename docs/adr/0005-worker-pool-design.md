# ADR 0005: Worker Pool Design

## Status
Accepted

## Context
Task execution should scale horizontally. Multiple workers should be able to join and leave the pool dynamically, subscribe to different sets of task handlers, and throttle how many tasks they execute simultaneously based on their hardware capacity.

## Decision
We implement a **dynamic worker pool with active heartbeats and capacity-aware scheduling**:
1.  **Registration**: Workers register with their supported handlers and a `maxConcurrent` capacity.
2.  **Heartbeats**: Workers must hit the `/api/workers/:workerId/heartbeat` endpoint periodically. Schedulers run a cron job to detect workers whose last heartbeat is older than 60 seconds and mark them `offline`.
3.  **Task Dispatching**: When polling for tasks, the scheduler finds eligible workers for the task handler.
4.  **Capacity Sorting**: Available workers are sorted by current capacity (`currentTasks / maxConcurrent`). The scheduler assigns the task to the worker with the lowest capacity usage (load balancing).

## Consequences
*   **Pros**:
    *   Load balances tasks evenly across online workers.
    *   Automatically cleans up dead or crashed worker references after 60 seconds.
    *   Supports heterogeneous workers (e.g. some workers handling only CPU-intensive tasks, others handling email).
*   **Cons**:
    *   If workers fail to send heartbeats due to networking lag (but are still running), they may be incorrectly marked offline, leaving running tasks in a "processing" state.
