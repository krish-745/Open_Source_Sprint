# PR: Fix #39 — Implement Distributed Tracing with OpenTelemetry-compatible correlation IDs

## Summary

Tasks that spawn subtasks or cross process boundaries had no way to correlate related log lines or find all tasks belonging to the same logical operation. There were no trace IDs, no span hierarchy, and no trace-indexed Redis lookup. This PR wires distributed tracing end-to-end across the task lifecycle, allowing developers to reconstruct call trees and trace task execution paths.

## Root Cause

The core `Task` interface was missing tracing fields. When tasks were created or executed, no tracing context was attached. 

```ts
// ❌ Before: no trace context anywhere
const task: Task = { id, name, handler, ... }; 
handler(task.payload);
```

Because of this, logs emitted during task creation, status transitions, retries, and execution carried only `taskId`. This made cross-process correlation impossible. 

```ts
// ❌ Before: logs lacked correlation IDs
logger.info({ taskId }, 'Task created');
logger.info({ taskId, duration }, 'Task completed');
```

Furthermore, `logger.ts` had no mechanism to bind OpenTelemetry-compatible fields to a log record, meaning we couldn't easily export these logs to tracing backends.

## Solution

### 1. New Tracing Fields on Task

We started by extending the `Task` interface in `src/types/index.ts` to include standard tracing identifiers.

```ts
// ✅ After: Task interface updated
traceId?: string;      // Shared by all spans in the chain
parentSpanId?: string; // Direct dependency's task ID
```

The `traceId` represents the entire chain of execution, while the `parentSpanId` allows us to reconstruct the exact call tree.

### 2. OpenTelemetry-Compatible Logger Helper

To make emitting these fields easy, we added a new `TraceContext` interface and a `withTrace` helper function to `src/utils/logger.ts`.

```ts
// ✅ After: TraceContext interface
export interface TraceContext {
  trace_id: string;
  span_id: string;
  parent_span_id?: string;
}
```

The field names intentionally match the OpenTelemetry Log Data Model. The `withTrace` helper returns a Pino child logger that pre-binds these fields.

```ts
// ✅ After: withTrace helper
export function withTrace(ctx: TraceContext): pino.Logger {
  return logger.child(ctx);
}
```

This ensures that every log line emitted from this child logger will automatically carry the correct `trace_id` and `span_id`.

### 3. Trace Generation and Propagation

In `src/services/task-queue.ts`, the `createTask` method now resolves the trace context before building the task object. If a task has dependencies, it inherits the trace context from the first one.

```ts
// ✅ After: Inheriting trace context
const depTask = await this.getTask(options.dependencies[0]);
if (depTask) {
  traceId = traceId || depTask.traceId;
  parentSpanId = parentSpanId || depTask.id;
}
```

If the task has no dependencies, it acts as a root span and generates a fresh `traceId` using `uuidv4()`.

### 4. Redis Trace Indexing

To support querying all tasks within a trace, we index the tasks in Redis sets. We also apply a TTL to these sets to prevent unbounded memory growth over time.

```ts
// ✅ After: Redis set with TTL
const traceKey = `trace:${traceId}`;
await client.sAdd(traceKey, taskId);
await client.expire(traceKey, TRACE_INDEX_TTL_SECONDS);
```

We exposed this data via a new public method, `getTasksByTraceId(traceId)`, which retrieves the IDs from the set and resolves them into full `Task` objects.

### 5. Logging with Trace Context

Throughout `task-queue.ts`, all task lifecycle log lines (creation, status updates, retries) now use the new `withTrace` helper.

```ts
// ✅ After: Lifecycle logging
withTrace({ trace_id, span_id, parent_span_id })
  .info({ taskId, queueName }, 'Task created');
```

This guarantees consistent OTel-compatible output across the entire system.

### 6. Passing Context to Handlers

Finally, in `src/services/task-executor.ts`, we updated the `TaskContext` interface to include `parentSpanId`.

```ts
// ✅ After: TaskContext updated
export interface TaskContext {
  taskId: string;
  traceId?: string;
  parentSpanId?: string;
}
```

The `execute` method now passes this full context to the registered handler, empowering handlers to forward the trace context when spawning their own child tasks. 

```ts
// ✅ After: Passing context to handler
const context: TaskContext = { taskId, traceId, parentSpanId };
const result = await Promise.race([ 
  handler(task.payload, context), 
  timeoutPromise 
]);
```

## Testing

Added `src/services/__tests__/task-queue.test.ts` (Redis mocked):

| Edge Case Tested | What it exercises |
|---|---|
| Root task generates a new traceId | Verifies that a task with no dependencies correctly generates a non-empty string for `traceId`, and leaves `parentSpanId` undefined. |
| Child inherits traceId + parentSpanId | Verifies that a child task pulls its `traceId` from its first dependency, and sets `parentSpanId` to that dependency's ID. |
| Explicit override wins | Verifies that passing explicit `traceId` and `parentSpanId` values in the options overrides any auto-derived values. |
| TTL is set on the trace index | Verifies that `expire` is called immediately after `sAdd`, ensuring the Redis sets do not grow unbounded forever. |
| `getTasksByTraceId` happy path | Verifies that `sMembers` resolves to IDs, and each ID is correctly fetched and returned as a full `Task` object. |
| `getTasksByTraceId` unknown traceId | Verifies that querying a non-existent trace returns an empty array and does not throw an error. |

`npm test` passes. `npm run type-check` passes.

Fixes #39
