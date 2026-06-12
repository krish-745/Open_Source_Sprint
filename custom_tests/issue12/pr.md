# PR: Fix #12 — Implement Task Rate Limiting

## Summary
A single queue could previously be flooded with millions of tasks, overwhelming workers. This PR introduces a highly precise, per-worker/queue token bucket rate limiter. It enforces maximum tasks-per-second constraints cleanly and handles atomic concurrency, ensuring worker performance is tightly controlled without impacting other workers or queues.

## Root Cause
Workers blindly pulled from queues via `assignTask` without any speed governance. If a high-volume burst occurred, workers would attempt to consume as fast as Redis and their internal handlers allowed, completely saturating the worker pools.

## Solution

Implemented `configureRateLimit`, `getRateLimit`, and `consumeRateLimitToken` in `TaskQueue`, and gated `WorkerPool.assignTask` with the rate limiter.

```ts
// WorkerPool.assignTask
if (task.queueName) {
  const allowed = await TaskQueue.consumeRateLimitToken(task.queueName, workerId);
  if (!allowed) {
    throw new Error(`Rate limit exceeded for queue ${task.queueName}`);
  }
}
```

- Users can now configure limits using `TaskQueue.configureRateLimit(queueName, maxTasksPerSecond)`.
- When assigning a task, the pool consumes a token via an atomic Lua script in Redis.
- If tokens are exhausted, the assignment is blocked, the queue is unmodified, and a metric violation is recorded.

### Edge Cases Handled

- **Atomic Concurrency & Precision Math:** Token bucket logic is evaluated within a single atomic Redis Lua script. Token refill math is calculated based on exact millisecond deltas, allowing smooth fractional token recovery.
- **Worker/Queue Isolation:** Tokens are tracked in strictly namespaced keys (`queue:<name>:worker:<id>:tokens`). One worker exhausting its quota on a queue will *never* starve another worker reading from the exact same queue.
- **Refill Clock Theft Prevention:** When a rate limit token is denied, the Lua script preserves the fractional token count but **does not advance** the `last_refill` timestamp. This crucially prevents aggressive polling loops from repeatedly stealing the refill time delta and permanently starving the client.
- **Memory Leak Protection:** Enforced a dynamic rolling TTL on the Redis token and timestamp keys. If a worker goes offline, its rate limit keys automatically expire, preventing unbounded memory leaks in Redis.
- **Zero Limits & Guards:** Gracefully handles missing queue names (passes through) and automatically blocks all assignments if limits are updated to `<= 0`.
- **Accurate Metric Tracking:** Rate limit violations increment the `rateLimitViolations` field on queue stats strictly only when the Lua script actually returns a denied state.

## Testing

Added comprehensive, rigid integration tests in `src/services/__tests__/rate-limit.test.ts` testing against a real Redis instance.

| Test | What it covers |
|---|---|
| Stores limits per queue | Verifies that `configureRateLimit` and `getRateLimit` correctly isolate configs across different queues. |
| Throttles exactly at limit | Validates that a worker gets exactly `LIMIT` tasks assigned and strictly no more in a rapid burst. |
| Token bucket refills | Simulates a $>1$ second pause and verifies the exact `LIMIT` of tokens correctly refill and are consumable again. |
| Exact violation metrics | Asserts that `queue:stats` accurately records the precise number of denied attempts. |
| Strict Worker Isolation | Worker 1 exhausts its bucket; verifies Worker 2 can still immediately consume its full bucket from the same queue. |
| Unlimited fallback | When no limit is configured, confirms 100% of tasks pass through without errors. |

`npm test` passes cleanly.

*Note: Also updated the test mock in `task-queue.batch.test.ts` and `worker-pool.test.ts` to support missing Redis methods (`zCard`, `hGet`, `hIncrBy`, `eval`) caused by recent upstream merges and this feature.*

Fixes #12
