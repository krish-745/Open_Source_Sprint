# PR: Fix #8 — Add Distributed Lock Timeout & Renewal Mechanism

> **Note to Maintainers:** This PR is built on top of the branch for **#44** to avoid merge conflicts, as both issues modify the same lock acquisition block in `TaskScheduler`. Once #44 is merged, this PR's diff will automatically update to show only the lock renewal changes.

## Summary
The `TaskScheduler` daemon relied on a static 10-second distributed lock TTL with no renewal mechanism. If processing a batch of scheduled tasks took longer than 10 seconds, the lock would silently expire mid-flight, allowing a second instance to acquire the lock and duplicate task processing. This PR introduces a configurable TTL and a `setInterval`-based lock renewal mechanism that atomically extends the lock while the scheduler holds it.

## Root Cause

```ts
const acquired = await client.set(lockKey, lockId, {
  NX: true,
  EX: 10, // ❌ Hardcoded. Lock expires even if still processing.
});
// No renewal. If the task loop takes > 10s, another instance acquires the lock.
```

## Solution

**1. Configurable TTL**
Added `lockTtlSeconds` param to `startScheduler` (default `10`). The value is passed directly to Redis `EX`, making it trivially overridable per deployment.

**2. Lock Renewal Mechanism with Lua Ownership Check**
A `setInterval` timer fires at `lockTtlSeconds / 2` and calls an atomic Lua script that verifies we still own the lock before extending the TTL. This is critical — without the ownership check, a renewal ping from a slow instance could accidentally renew a lock already taken over by a healthy instance.

```ts
const renewScript = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("expire", KEYS[1], ARGV[2])
  else
    return 0
  end
`;

const renewalInterval = setInterval(async () => {
  await client.eval(renewScript, {
    keys: [lockKey],
    arguments: [lockId, lockTtlSeconds.toString()]
  });
}, (lockTtlSeconds * 1000) / 2); // ✅ Fires at 50% of TTL
```

**3. Guaranteed Cleanup via `finally`**
The `clearInterval` and atomic lock release both live inside a `finally` block — guaranteeing the renewal interval is always cancelled and the lock always released, even if the task processing loop throws mid-batch.

```ts
try {
  // Process tasks...
} finally {
  clearInterval(renewalInterval); // ✅ Stop renewal before releasing lock
  await client.eval(releaseScript, { keys: [lockKey], arguments: [lockId] });
}
```

Note: `clearInterval` runs *before* lock release intentionally — this prevents a racing renewal callback from trying to extend a lock we are actively deleting.

## Testing

Added tests to `src/services/__tests__/task-scheduler.test.ts` (Redis mocked):

| Test | What it covers |
|---|---|
| Configurable TTL | Verifies `EX` in the `SET` command matches the passed `lockTtlSeconds` |
| Lock Renewal Mechanism | Simulates a long-running batch (frozen `zRange` promise); verifies `eval` with `expire` fires at TTL/2 |
| Lock Takeover | Simulates a crashed holder; verifies another instance successfully acquires after the TTL window |

`npm test` passes. `npm run type-check` passes.

## Known Limitation
If the renewal mechanism's Redis call consistently fails due to a network partition, the lock will eventually expire and two instances could briefly process the same tasks. This is the **accepted tradeoff** of all time-based distributed locks (including the official Redlock spec). Addressing this fully would require distributed consensus, which is out of scope for this issue.

Fixes #8
