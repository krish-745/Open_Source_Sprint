# PR: Fix #22 — Implement backpressure to reject new tasks when queue exceeds max size

## Summary
`TaskQueue.createTask()` added tasks to the queue unconditionally with no upper bound check. When tasks were produced faster than workers could consume them, the Redis sorted set grew without limit — risking memory exhaustion and unbounded latency for callers. The API returned no signal to the producer that the system was overloaded.

## Root Cause

```ts
static async createTask(...): Promise<Task> {
  const client = getRedisClient();
  // ❌ No size check — queue grows unbounded
  const task: Task = { ... };
  await client.set(...);
  await client.zAdd(queueKey, { score, value: taskId });
}
```

Without a size cap, a burst of traffic could silently fill Redis memory. Callers received `201 Created` indefinitely with no way to know the system was overwhelmed.

## Solution

Introduce a `QueueFullError` typed error class and check the live queue depth via `zCard` before every insert. The `MAX_QUEUE_SIZE` environment variable controls the limit (defaults to `10000`). The API layer catches `QueueFullError` via `instanceof` and returns `HTTP 429 Too Many Requests`.

### `src/services/task-queue.ts`

```ts
/**
 * Thrown when a queue has reached its maximum capacity (backpressure).
 * The API layer maps this to HTTP 429 Too Many Requests.
 */
export class QueueFullError extends Error {
  constructor(queueName: string, maxSize: number) {
    super(`Queue ${queueName} exceeds maximum size of ${maxSize}`);
    this.name = 'QueueFullError';
  }
}
```

```ts
const queueKey = `${QUEUE_PREFIX}${queueName}`;
const maxQueueSize = parseInt(process.env.MAX_QUEUE_SIZE || '10000', 10);
const currentSize = await client.zCard(queueKey);

if (currentSize >= maxQueueSize) {
  throw new QueueFullError(queueName, maxQueueSize);   // ✅ backpressure enforced
}
```

### `src/routes/api.ts`

```ts
import { TaskQueue, QueueFullError } from '../services/task-queue';

// ...

} catch (error: any) {
  if (error instanceof QueueFullError) {          // ✅ typed check, not string matching
    return res.status(429).json({ error: error.message });
  }
  res.status(500).json({ error: error.message });
}
```

Using `instanceof` instead of `error.message.includes(...)` ensures the 429 mapping is refactor-safe — renaming the error message string in the future cannot silently break the status code.

This also aligns with the existing `SchedulerConfig.maxQueueSize` and `Queue.maxSize` fields already defined in `src/types/index.ts`, confirming backpressure was always an intended design feature that was never wired up.

## Testing

Added tests to `src/services/__tests__/task-queue.test.ts` (Redis mocked):

| Test | What it covers |
|---|---|
| Rejects task when at limit | `zCard` returns `maxQueueSize` → `QueueFullError` thrown |
| Allows task creation below limit | `zCard` returns `maxQueueSize - 1` → task created successfully |

`npm test` passes. `npm run type-check` passes.

Fixes #22
