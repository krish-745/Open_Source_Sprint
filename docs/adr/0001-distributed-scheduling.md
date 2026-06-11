# ADR 0001: Distributed Task Scheduling Approach

## Status
Accepted

## Context
TaskFlow is designed to run in a multi-instance, distributed environment where multiple scheduler and API instances might run concurrently. We need a reliable mechanism to ensure tasks are scheduled, delays are handled, and cron-like recurring schedules are evaluated without duplicate executions or race conditions between different scheduler instances.

## Decision
We choose a **Redis-based Distributed Lock (Redlock-like pattern)** approach for task scheduling coordination:
1. Schedulers run periodically (e.g. every second) to poll for ready delayed/recurring tasks.
2. Schedulers attempt to acquire a global distributed lock in Redis with a short time-to-live (TTL, e.g. 10 seconds) before performing scheduling runs.
3. Only the instance holding the lock can parse scheduled/recurring rules and transition tasks from the pending/scheduled states to the active queue.
4. Active task execution is worker-driven: once tasks are placed in the queue, workers grab tasks via atomic operations, ensuring execution load is distributed.

## Consequences
*   **Pros**:
    *   Prevents duplicate task creation/queueing from multiple scheduler instances.
    *   Low overhead since Redis operations are highly optimized.
    *   Provides active-passive style high-availability for the scheduler logic itself.
*   **Cons**:
    *   If a scheduler instance crashes while holding the lock, scheduling halts until the lock TTL expires.
    *   Relies heavily on clock sync across machines if lock TTLs are small.
