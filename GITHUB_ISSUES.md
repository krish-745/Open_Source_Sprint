# TaskFlow - GitHub Issues for Open Source Sprint 2026

## Priority: CRITICAL (Must fix first - blocks other work)

### Issue #1: Fix Race Condition in Task Status Update
**Difficulty: HIGH**
**Description:**
When multiple workers attempt to update the same task status simultaneously, there's a race condition in `TaskQueue.updateTaskStatus()`. The current implementation reads, modifies, and writes back without atomicity. This can result in lost updates or inconsistent state.

**Steps to Reproduce:**
1. Create a task
2. Have 3+ workers process it simultaneously
3. Observe status updates can be lost

**Expected:** Atomic status updates using Redis transactions
**Current:** Non-atomic read-modify-write

**Acceptance Criteria:**
- Use Redis WATCH/MULTI/EXEC or Lua scripts
- Add test for concurrent status updates
- No status updates should be lost

**Files to Modify:** src/services/task-queue.ts

---

### Issue #2: Deadlock Detection in Task Dependencies
**Difficulty: VERY HIGH**
**Description:**
When tasks form circular dependencies (A → B → C → A), the system hangs indefinitely waiting for dependencies that will never complete. There's no cycle detection in `TaskQueue._checkDependencies()`.

**Acceptance Criteria:**
- Detect circular dependencies at task creation time
- Reject tasks with cycles with clear error message
- Return HTTP 400 with cycle details
- Add DFS-based cycle detection algorithm
- Handle transitive dependencies correctly

**Files to Modify:** src/services/task-queue.ts, src/routes/api.ts

---

### Issue #3: Worker Crash Handling - Orphaned Tasks
**Difficulty: HIGH**
**Description:**
When a worker crashes unexpectedly, tasks assigned to it remain in "processing" state forever. They never timeout or retry. Need to implement stale task detection.

**Acceptance Criteria:**
- Detect tasks stuck in "processing" for too long
- Automatically reassign to available workers
- Add configurable timeout for stuck tasks
- Log detailed information about recovered tasks

**Files to Modify:** src/services/task-queue.ts, src/services/metrics-collector.ts

---

## Priority: HIGH (Important functionality)

### Issue #4: Implement Task Timeout Enforcement
**Difficulty: HIGH**
**Description:**
Tasks should automatically be killed if they exceed their timeout. Currently, `TaskExecutor.execute()` has a timeout promise, but it doesn't guarantee immediate termination of the handler function itself.

**Acceptance Criteria:**
- Handler execution must be forcefully terminated after timeout
- Worker resources properly cleaned up
- Task marked as failed with clear timeout error
- No resource leaks from killed tasks
- Add integration test with slow handlers

**Files to Modify:** src/services/task-executor.ts, src/services/worker-pool.ts

---

### Issue #5: Redis Connection Failure Recovery
**Difficulty: MEDIUM**
**Description:**
If Redis temporarily disconnects, the application doesn't properly reconnect or queue operations. Current reconnection strategy only logs but doesn't retry failed operations.

**Acceptance Criteria:**
- Queue operations during disconnection
- Retry queued operations once reconnected
- Exponential backoff for reconnection
- Maximum retry limits
- Health check endpoint reports Redis status

**Files to Modify:** src/services/redis.ts, src/routes/api.ts

---

### Issue #6: Memory Leak in Metrics Snapshots
**Difficulty: MEDIUM**
**Description:**
`MetricsCollector` stores snapshots with TTL set to 7 days, but snapshots accumulate quickly. With many workers and queues, this can consume significant memory. Need to implement snapshot retention policy with configurable limits.

**Acceptance Criteria:**
- Limit number of stored snapshots (e.g., keep last 1000)
- Implement sliding window of metrics
- Configurable retention size and time
- Automatic cleanup of old snapshots
- Add memory usage monitoring

**Files to Modify:** src/services/metrics-collector.ts

---

### Issue #7: Task Priority Inversion Problem
**Difficulty: HIGH**
**Description:**
A low-priority task can block a high-priority task if the low-priority task is already running and there are no available workers. The queue processing in `TaskQueue.getNextTask()` doesn't account for worker availability vs. true queue position.

**Acceptance Criteria:**
- High-priority tasks should preempt lower-priority ones when workers are available
- Implement priority queue correctly
- Add test showing priority task executes before queued lower-priority
- Document priority semantics

**Files to Modify:** src/services/task-queue.ts

---

### Issue #8: Add Distributed Lock Timeout
**Difficulty: MEDIUM**
**Description:**
The scheduler uses `TaskScheduler` with a 10-second lock timeout. If a scheduler process crashes while holding the lock, no other instance can acquire it for 10 seconds. Should implement automatic lock expiration with renewal.

**Acceptance Criteria:**
- Implement lock renewal mechanism
- Process should renew lock before expiration
- Configurable lock TTL
- Detect crashed lock holders
- Add test for lock takeover

**Files to Modify:** src/services/task-scheduler.ts

---

## Priority: MEDIUM (Nice to have, improves robustness)

### Issue #9: Implement Batch Task Creation
**Difficulty: MEDIUM**
**Description:**
Creating tasks one-by-one through multiple API calls is inefficient. Need bulk/batch endpoint that creates multiple tasks atomically.

**Acceptance Criteria:**
- POST /api/tasks/batch endpoint
- Create up to 1000 tasks in single call
- All or nothing atomic behavior
- Return array of created task IDs
- Validate all before creating any

**Files to Modify:** src/routes/api.ts, src/services/task-queue.ts

---

### Issue #10: Task Result Compression
**Difficulty: MEDIUM**
**Description:**
Large task results (MB-sized) stored in Redis waste memory. Should compress results using gzip before storage and decompress on retrieval.

**Acceptance Criteria:**
- Compress results when storing (> 10KB)
- Decompress transparently on retrieval
- Add compression ratio metrics
- Handle compression errors gracefully
- Backward compatible with uncompressed results

**Files to Modify:** src/services/task-queue.ts

---

### Issue #11: Implement Task Cancellation
**Difficulty: HIGH**
**Description:**
There's no way to cancel a running task. Need to implement graceful cancellation with cleanup.

**Acceptance Criteria:**
- POST /api/tasks/{taskId}/cancel endpoint
- Gracefully stop execution (signal to handler)
- Cleanup worker resources
- Mark task as 'cancelled' status
- Cannot cancel already completed tasks
- Handler should check cancellation signal

**Files to Modify:** src/services/task-executor.ts, src/services/task-queue.ts, src/routes/api.ts

---

### Issue #12: Add Task Rate Limiting
**Difficulty: MEDIUM**
**Description:**
A single queue can be flooded with millions of tasks, overwhelming workers. Need rate limiting per worker/queue combination.

**Acceptance Criteria:**
- Configure max tasks per second per queue
- Throttle task assignment when limit reached
- Implement token bucket algorithm
- Add metrics for rate limit violations
- Configure per queue

**Files to Modify:** src/services/task-queue.ts, src/services/worker-pool.ts

---

### Issue #13: Implement Task Filtering in API
**Difficulty: LOW**
**Description:**
The `/api/queues/{queueName}/tasks` endpoint returns all tasks regardless of status. Need filtering by status, priority, tags, etc.

**Acceptance Criteria:**
- Filter by status (pending, processing, completed, failed)
- Filter by priority
- Filter by tags (support multiple)
- Filter by date range
- Combine multiple filters with AND logic
- Document filter syntax

**Files to Modify:** src/services/task-queue.ts, src/routes/api.ts

---

### Issue #14: Add Task Search by Name/Description
**Difficulty: LOW**
**Description:**
Tasks can only be retrieved by ID. Need full-text search capability for task names and descriptions.

**Acceptance Criteria:**
- GET /api/tasks/search?q=search_term endpoint
- Search across task name and description
- Case-insensitive matching
- Return matching task IDs with relevance score
- Limit results to prevent performance issues
- Handle special characters safely

**Files to Modify:** src/routes/api.ts, src/services/task-queue.ts

---

## Bug Fixes

### Issue #15: Fix Worker Status Not Updating to Idle
**Difficulty: LOW**
**Description:**
In `TaskExecutor.execute()`, finally block calls `updateWorkerStatus(..., 'idle')` after task completes. But if multiple tasks complete in quick succession, the status might be incorrectly set to idle when worker should be busy.

**Reproduction:**
1. Create worker that processes multiple tasks
2. Complete tasks rapidly
3. Check worker status in metrics

**Expected:** Status should reflect actual state

**Files to Modify:** src/services/task-executor.ts

---

### Issue #16: Fix Queue Stats Not Decrementing
**Difficulty: MEDIUM**
**Description:**
In `TaskQueue.updateTaskStatus()`, when a task moves from "pending" to "queued" or "processing", the stats are never updated. The queue shows tasks as pending forever.

**Acceptance Criteria:**
- Update stats when task status changes
- Decrement pending count when queued
- Increment processing count when processing
- Maintain stats consistency
- Add test for stats accuracy

**Files to Modify:** src/services/task-queue.ts

---

### Issue #17: Fix MetricsCollector Snapshot Keys Sorting
**Difficulty: LOW**
**Description:**
In `MetricsCollector.getLatestSnapshot()`, keys are sorted lexicographically, but timestamps are still comparable this way due to numeric format. However, this approach is fragile and not guaranteed to work correctly.

**Acceptance Criteria:**
- Use numeric comparison for timestamps
- Extract timestamp from key before sorting
- Add test with timestamps close together
- Document sorting behavior

**Files to Modify:** src/services/metrics-collector.ts

---

### Issue #18: Fix Task Retry Not Clearing Error Message
**Difficulty: LOW**
**Description:**
When `TaskQueue.retryTask()` creates a retry attempt, it sets `task.error = undefined`, but if the error property wasn't in the original object, this could leave undefined fields.

**Acceptance Criteria:**
- Delete error field properly (not set to undefined)
- Ensure clean task state before retry
- Add test for retried task error handling

**Files to Modify:** src/services/task-queue.ts

---

## Architecture & Performance

### Issue #19: Implement Connection Pooling for Redis
**Difficulty: MEDIUM**
**Description:**
Current Redis client is a single instance. Under high load, connection limits can be reached. Need connection pooling.

**Acceptance Criteria:**
- Implement connection pool with min/max size
- Queue requests when pool exhausted
- Configurable pool size
- Monitor pool utilization
- Add health checks per connection

**Files to Modify:** src/services/redis.ts

---

### Issue #20: Optimize Queue Queries with Indexes
**Difficulty: HIGH**
**Description:**
Querying tasks by status requires scanning all tasks. With millions of tasks, this is slow. Need efficient indexing strategy.

**Acceptance Criteria:**
- Index tasks by status
- Index tasks by queue + status
- Index tasks by creation time
- Query times should be O(1) or O(log n)
- Maintain index consistency
- Document index structure

**Files to Modify:** src/services/task-queue.ts

---

### Issue #21: Add Task Batching in Worker Execution
**Difficulty: HIGH**
**Description:**
Workers process one task at a time. Implement batch processing where workers can fetch and process multiple tasks together (e.g., for bulk operations).

**Acceptance Criteria:**
- Worker can specify batch size
- Executor fetches multiple tasks together
- Handlers receive batch of tasks
- Partial batch failure handling
- Performance improvement metrics

**Files to Modify:** src/services/task-executor.ts, src/services/task-queue.ts, src/services/worker-pool.ts

---

### Issue #22: Implement Backpressure in Queue
**Difficulty: MEDIUM**
**Description:**
When tasks are created faster than workers can process, the queue grows unbounded. Need backpressure mechanism.

**Acceptance Criteria:**
- Reject new tasks if queue exceeds max size
- Return HTTP 429 (Too Many Requests) when backpressured
- Configurable queue size limit
- Clear error message to caller
- Document backpressure behavior

**Files to Modify:** src/services/task-queue.ts, src/routes/api.ts

---

## Features & Enhancements

### Issue #23: Implement Task Monitoring/Observability Hooks
**Difficulty: MEDIUM**
**Description:**
Need callbacks/hooks that fire at various task lifecycle points for external monitoring/logging systems.

**Acceptance Criteria:**
- Hooks for task created, started, completed, failed, retried
- Register custom hook handlers
- Hooks receive full task context
- Error in hook doesn't break execution
- Add hooks for worker events too

**Files to Modify:** src/services/task-executor.ts, src/services/task-queue.ts, src/services/worker-pool.ts

---

### Issue #24: Implement Scheduled Task Modification
**Difficulty: MEDIUM**
**Description:**
Once a task is scheduled, you can't modify its parameters (priority, timeout, retry count, etc.). Need update capability.

**Acceptance Criteria:**
- PATCH /api/tasks/{taskId} endpoint
- Allow modifying fields before execution
- Prevent modification of executing/completed tasks
- Validate modifications
- Audit trail of changes

**Files to Modify:** src/services/task-queue.ts, src/routes/api.ts

---

### Issue #25: Implement Dead Letter Queue Inspection
**Difficulty: MEDIUM**
**Description:**
Dead letter queue has no API. Need endpoints to inspect, retry, or delete failed tasks from DLQ.

**Acceptance Criteria:**
- GET /api/dlq to list failed tasks
- POST /api/dlq/{taskId}/retry to retry from DLQ
- DELETE /api/dlq/{taskId} to remove
- Filtering support (time range, error type)
- DLQ statistics

**Files to Modify:** src/services/task-queue.ts, src/routes/api.ts

---

### Issue #26: Implement Task Grouping
**Difficulty: HIGH**
**Description:**
No way to group related tasks together. Need ability to tag tasks and track completion of groups.

**Acceptance Criteria:**
- Assign group ID to tasks during creation
- GET /api/groups/{groupId}/status endpoint
- Track completion percentage of group
- Query all tasks in group
- Aggregate results across group

**Files to Modify:** src/services/task-queue.ts, src/routes/api.ts, src/types/index.ts

---

### Issue #27: Implement Task Callback URLs
**Difficulty: MEDIUM**
**Description:**
When task completes, should be able to HTTP POST result to a webhook URL provided by caller.

**Acceptance Criteria:**
- Task accepts optional callbackUrl field
- On completion, POST result to URL
- Retry callback if delivery fails
- Timeout for callback delivery
- Include task metadata in callback payload

**Files to Modify:** src/services/task-executor.ts, src/services/task-queue.ts, src/types/index.ts

---

### Issue #28: Implement Task Templating
**Difficulty: MEDIUM**
**Description:**
Need to create reusable task templates with default values and payload schema validation.

**Acceptance Criteria:**
- Create task templates with schema
- Validate payload against schema before creating task
- Default values filled automatically
- POST /api/templates endpoint
- Use templates when creating tasks

**Files to Modify:** src/routes/api.ts, src/types/index.ts

---

### Issue #29: Implement Worker Auto-Scaling
**Difficulty: HIGH**
**Description:**
Should automatically spawn/kill workers based on queue depth and load.

**Acceptance Criteria:**
- Monitor queue size and task processing rate
- Scale up if queue growing and capacity low
- Scale down if queue empty
- Configurable thresholds
- Prevent thrashing (min time between scale events)

**Files to Modify:** src/services/metrics-collector.ts, src/services/worker-pool.ts

---

### Issue #30: Implement Task Time-to-Live (TTL)
**Difficulty: MEDIUM**
**Description:**
Tasks should expire if not picked up within a certain time window.

**Acceptance Criteria:**
- Task accepts ttl field (seconds)
- Expired pending tasks are automatically cancelled
- Different TTL for different priorities
- Metrics for expired tasks
- Prevent execution of expired tasks

**Files to Modify:** src/services/task-queue.ts, src/services/task-executor.ts

---

## Testing & Quality

### Issue #31: Add Comprehensive Unit Tests for Task Queue
**Difficulty: MEDIUM**
**Description:**
`task-queue.ts` has no test coverage. Need unit tests for all public methods.

**Acceptance Criteria:**
- Tests for createTask, getTask, updateTaskStatus
- Tests for getNextTask with dependencies
- Tests for retryTask with max retries
- Tests for cleanupOldTasks
- Minimum 80% line coverage
- Mock Redis client

**Files to Modify:** src/services/__tests__/task-queue.test.ts

---

### Issue #32: Add Integration Tests for Worker Pool
**Difficulty: MEDIUM**
**Description:**
Worker pool operations need integration tests with real Redis.

**Acceptance Criteria:**
- Tests for registerWorker, getWorker
- Tests for assignTask and completeTask
- Tests for stale worker detection
- Tests for handler mapping
- Use testcontainers or local Redis

**Files to Modify:** src/services/__tests__/worker-pool.test.ts

---

### Issue #33: Add End-to-End Tests for Task Lifecycle
**Difficulty: HIGH**
**Description:**
Need E2E tests showing complete task journey from creation to completion.

**Acceptance Criteria:**
- Test create → queue → assign → execute → complete flow
- Test retry flow
- Test dependency satisfaction
- Test timeout handling
- Test error scenarios
- Use full stack (HTTP API, Redis, task executor)

**Files to Modify:** src/__tests__/e2e.test.ts

---

### Issue #34: Add Load Tests
**Difficulty: HIGH**
**Description:**
No load testing. Need baseline metrics for throughput and latency.

**Acceptance Criteria:**
- Create tasks at high rate (1000 tasks/sec)
- Measure queue latency
- Measure task completion time
- Identify bottlenecks
- Document baseline metrics
- Identify breakpoints

**Files to Modify:** src/__tests__/load.test.ts

---

## Documentation

### Issue #35: Add API Documentation
**Difficulty: LOW**
**Description:**
No API documentation. Need OpenAPI/Swagger spec.

**Acceptance Criteria:**
- OpenAPI 3.0 specification
- Document all endpoints
- Include request/response schemas
- Provide example requests
- Generate Swagger UI

**Files to Modify:** Create openapi.yml or use swagger annotations

---

### Issue #36: Add Architecture Decision Records (ADRs)
**Difficulty: LOW**
**Description:**
Document design decisions for future maintainers.

**Acceptance Criteria:**
- ADR for distributed task scheduling approach
- ADR for Redis data structures chosen
- ADR for retry strategy
- ADR for metrics collection
- ADR for worker pool design

**Files to Modify:** Create docs/adr/ directory

---

### Issue #37: Add Operational Runbook
**Difficulty: LOW**
**Description:**
Need documentation for operating the system in production.

**Acceptance Criteria:**
- How to deploy
- How to scale workers
- How to monitor health
- How to handle failures
- How to debug issues
- Troubleshooting guide

**Files to Modify:** Create docs/operations.md

---

## Advanced Features

### Issue #38: Implement Task Priorities with SLA
**Difficulty: HIGH**
**Description:**
High-priority tasks should have guaranteed SLA (99% processed within X seconds). Implement priority SLA enforcement.

**Acceptance Criteria:**
- Define SLA per priority level
- Track SLA compliance metrics
- Alert when SLA violated
- Preempt low-priority tasks if high-priority SLA at risk
- Document SLA strategy

**Files to Modify:** src/services/task-scheduler.ts, src/services/metrics-collector.ts

---

### Issue #39: Implement Distributed Tracing
**Difficulty: HIGH**
**Description:**
Tasks spawning subtasks or crossing process boundaries need correlation IDs for tracing.

**Acceptance Criteria:**
- Generate trace ID per task
- Propagate through dependency chain
- Emit trace logs with correlation IDs
- Support OpenTelemetry export
- Query tasks by trace ID

**Files to Modify:** src/services/task-queue.ts, src/services/task-executor.ts, src/utils/logger.ts

---

### Issue #40: Implement Task Consensus (Multi-Quorum)
**Difficulty: VERY HIGH**
**Description:**
For critical tasks, implement execution on multiple workers with consensus verification that all got same result.

**Acceptance Criteria:**
- Task can specify quorum requirement (e.g., 3 workers)
- Execute on multiple workers simultaneously
- Compare results (hash)
- Mark success only if quorum agrees
- Handle disagreements (byzantine fault tolerance basic)
- Different consensus strategies (all match, majority, weighted)

**Files to Modify:** src/services/task-executor.ts, src/services/worker-pool.ts, src/types/index.ts

---

### Issue #41: Implement Conditional Task Branching
**Difficulty: MEDIUM**
**Description:**
Currently task dependencies are linear. Need branching where task result determines next task to run.

**Acceptance Criteria:**
- Task can specify conditional branches
- Result is evaluated against conditions
- Different tasks run based on conditions
- Multiple branches possible
- Support regexp matching on results

**Files to Modify:** src/services/task-queue.ts, src/types/index.ts

---

### Issue #42: Implement Cost-Based Scheduling
**Difficulty: HIGH**
**Description:**
Tasks should have cost (CPU, memory, money) and scheduler should minimize total cost while meeting deadlines.

**Acceptance Criteria:**
- Task specifies cost metadata
- Scheduler tracks worker costs
- Choose workers to minimize cost
- Budget constraints per caller
- Cost metrics reporting
- Cost prediction for queued tasks

**Files to Modify:** src/services/task-queue.ts, src/services/worker-pool.ts, src/services/metrics-collector.ts

---

## Critical Issues (Found During Testing)

### Issue #43: Handle Empty Payload Gracefully
**Difficulty: LOW**
**Description:**
If task created with empty/null payload, system may crash when executor tries to pass to handler.

**Acceptance Criteria:**
- Accept empty payload ({} or null)
- Pass empty object to handler
- Add validation
- Document payload requirements

**Files to Modify:** src/services/task-queue.ts, src/routes/api.ts

---

### Issue #44: Prevent Concurrent Scheduler Instances from Duplicate Processing
**Difficulty: MEDIUM**
**Description:**
Multiple scheduler instances could process same scheduled task if lock handling has edge cases.

**Acceptance Criteria:**
- Prove only one instance processes each scheduled task
- Add test with multiple scheduler instances
- Show no duplicate processing
- Handle clock skew between machines

**Files to Modify:** src/services/task-scheduler.ts

---

### Issue #45: Handle Worker Disconnect During Task Execution
**Difficulty: MEDIUM**
**Description:**
If worker crashes mid-execution, task state becomes inconsistent.

**Acceptance Criteria:**
- Detect worker offline while task processing
- Mark task as stuck/failed
- Reassign to other workers
- Clear worker assignment
- Add test for this scenario

**Files to Modify:** src/services/worker-pool.ts, src/services/task-queue.ts

---

---

## Summary Statistics

- **Total Issues:** 45
- **Critical (VERY HIGH):** 2
- **Critical (HIGH):** 9
- **Important (MEDIUM):** 20
- **Nice-to-have (LOW):** 14

- **By Category:**
  - Bug Fixes: 5
  - Architecture & Performance: 6
  - Features & Enhancements: 10
  - Testing & Quality: 5
  - Documentation: 3
  - Advanced Features: 6
  - Race Conditions & Concurrency: 4
  - Operational Issues: 6

## Difficulty Distribution

- **VERY HIGH:** 2 issues (Expert level)
- **HIGH:** 14 issues (Advanced level)
- **MEDIUM:** 20 issues (Intermediate level)
- **LOW:** 9 issues (Beginner friendly)

## Getting Started for Contributors

### For Beginners
- Start with Issues #13-14 (API filtering)
- Then try #15-18 (Bug fixes)
- Then move to #31 (Unit tests)

### For Intermediate
- Try #9-12 (Feature additions)
- Then #25-27 (API enhancements)
- Then #19-20 (Optimization)

### For Advanced
- Try #3-8 (Complex bugs)
- Then #21-22 (Architecture)
- Then #38-42 (Advanced features)

### For Experts
- Try #2, #40, #42 (Research-level work)
- Concurrent issues that require deep system knowledge

---

**Note:** All issues should follow these guidelines:
1. Write tests before submitting PR
2. Update documentation if needed
3. Include performance metrics for optimization PRs
4. Run full test suite before submitting
5. Ensure no regressions in other systems
