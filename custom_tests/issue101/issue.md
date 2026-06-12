# Issue : Distributed Lock Race Condition during Release

## Description
In the `TaskScheduler` daemon (`src/services/task-scheduler.ts`), the mechanism for releasing the distributed lock is not atomic. The code currently reads:

```typescript
// Release lock
const currentLock = await client.get(lockKey);
if (currentLock === lockId) {
  await client.del(lockKey);
}
```

## The Problem
Because there are two separate round trips to Redis (`get` then `del`), a race condition exists:
1. **Worker A** checks the lock and gets its own `lockId` back.
2. Immediately after the `get`, the lock's TTL (`EX: 10`) expires.
3. **Worker B** acquires the lock with a new `lockId`.
4. **Worker A** executes `client.del(lockKey)`, mistakenly deleting **Worker B's** lock.
5. Now **Worker C** can acquire the lock. 

This violates the mutual exclusion property of the lock. If this occurs, multiple scheduler instances will run simultaneously, reading the same scheduled tasks and enqueueing them multiple times, leading to duplicate processing.

## Severity
**High**. This is a classic distributed systems bug that occurs more frequently under load or network latency and breaks the core guarantee of the `TaskScheduler` (only one instance migrating scheduled tasks to the active queue at a time).

## Proposed Fix
The check-and-delete operation must be performed atomically inside Redis. We should replace the `get` and `del` sequence with a tiny Lua script evaluated directly by Redis:

```lua
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
```
This guarantees that the lock is only deleted if it still belongs to the calling worker.
