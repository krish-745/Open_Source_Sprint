# 3. Retry strategy and dead letter queue

Status: Accepted

## Context
Task handlers can fail transiently (network, downstream outages) or permanently (bad input). The system needs bounded automatic retries and a way to inspect tasks that exhaust them.

## Decision
Each task carries `retries` and `maxRetries`. On failure (`TaskExecutor`), `TaskQueue.retryTask` is called:

- If `retries < maxRetries`: increment `retries`, set status `retry`, clear the previous error, and re-enqueue at the task's priority.
- If `retries >= maxRetries`: move the task to the dead letter queue (`dlq:tasks`) and mark it failed.

`maxRetries` defaults to 3 and is configurable per task at creation.

## Consequences
- Pros: transient failures recover automatically; permanently failing tasks are isolated for inspection rather than looping forever.
- Cons: retries are currently immediate (re-enqueued at the same priority) with no backoff, so a fast-failing task can be retried in quick succession. A future ADR may introduce exponential backoff / delayed re-enqueue.
- The DLQ is a list; a separate inspection/requeue API is tracked as a follow-up issue.
