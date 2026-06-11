# ADR 0003: Retry Strategy

## Status
Accepted

## Context
In a distributed environment, transient errors (network timeouts, database locking issues, temporary API failures) can cause tasks to fail. We need a robust retry strategy that tolerates transient errors without entering infinite retry loops or polluting task states.

## Decision
We implement a **bounded-attempt retry mechanism with clean state transition**:
1.  **Retry Limits**: Tasks specify a `maxRetries` (default: 3).
2.  **Retry Transition**: When task execution fails, the executor increments the task's `retries` count.
3.  **State Cleanup**: Before a task is re-queued, its temporary execution state (such as the last execution `error` message) is completely removed from the task object to ensure a clean state.
4.  **Priority Retention**: The task is put back onto its respective queue using its original priority score.
5.  **Dead Letter Queue (DLQ)**: If `retries` reaches `maxRetries`, the task is moved to `dlq:tasks` and marked as failed.

## Consequences
*   **Pros**:
    *   Tolerates temporary/transient worker or network errors.
    *   Prevents permanent errors from wasting worker CPU cycles indefinitely.
    *   Clearing errors prevents outdated error logs from confusing monitoring systems.
*   **Cons**:
    *   Currently relies on immediate re-queueing; there is no exponential backoff delay between retry attempts (future enhancement).
