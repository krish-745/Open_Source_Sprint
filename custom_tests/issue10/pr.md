# PR: Fix #10 — Implement Task Result Compression

## Summary
Large task results (MB-sized) stored in Redis were wasting significant memory. This PR introduces a transparent gzip-based compression pipeline to compress task results exceeding 10KB, reducing memory overhead while remaining completely backward compatible with existing uncompressed results. It also hardens the system against severe security and performance edge cases.

## Root Cause
Previously, any arbitrary payload assigned to `task.result` was blindly converted using `JSON.stringify()` and saved into Redis as-is. Extremely large task execution results would balloon the memory consumed by `task:*` keys in the database. 

## Solution

Implemented `_compressPayload` and `_decompressTask` in `TaskQueue`.

```ts
if (originalByteLength > 10 * 1024 || isSpoofed) {
  const compressed = await gzip(resultString);
  // ... metrics tracking ...
  return `__gz_json_b64__:${compressed.toString('base64')}`;
}
```

- When a task result is over 10KB, it is strictly `JSON.stringify`'d, gzip compressed, base64-encoded, and prefixed with `__gz_json_b64__:`.
- `TaskQueue.getTask()` transparently detects the prefix, unzips the payload, and parses the object to return exactly what was initially passed.

### Edge Cases Handled

- **Zip Bomb Memory Exhaustion:** Enforced `{ maxOutputLength: 50 * 1024 * 1024 }` on `gunzip` to protect the Node.js process from crashing via maliciously inflated compressed payloads.
- **Prefix Spoofing Bypass:** If a user submits a payload `<10KB` that attempts to maliciously impersonate our `__gz_json_b64__:` internal prefix, the pipeline securely forces compression on it so it safely deserializes back to their spoofed text upon retrieval.
- **Infinite Metrics Double-Counting & Performance Degradation:** Internal queue transitions (`_checkDependencies`, `_moveToDeadLetterQueue`, `retryTask`) now use a new `_getRawTask()` fetcher that completely avoids unnecessary decompression. Compression metrics strictly trigger only on net-new results.
- **Legacy Data Morphing:** Maintained fallback support for V1 (`__gz_b64__:`) prefixes with safety logic that refuses to `JSON.parse` legacy primitives, preventing older raw strings (e.g. `"true"`) from unpredictably morphing into actual booleans.
- **Side-Effect Prevention:** In `updateTaskStatus`, `metadata` is safely shallow-copied (`{ ...metadata }`) to guarantee caller-provided reference objects aren't accidentally mutated into base64 strings in the caller's scope.
- **Accurate Metric Calculations:** Metric hashing strictly uses `Buffer.byteLength(resultString, 'utf8')` instead of `.length` to guarantee that multi-byte strings and emojis are mathematically tracked using apples-to-apples byte sizes.

## Testing

Added `src/services/__tests__/task-queue.test.ts` (Redis mocked):

| Test | What it covers |
|---|---|
| Compresses large task results | Result `> 10KB` is stored strictly as `< 10KB` raw string. |
| Transparently decompresses results | Data retrieved perfectly matches the initial payload. |
| Correctly preserves large objects | Ensures strict type serialization for nested objects and arrays. |
| Stores small results uncompressed | Skips compression pipeline effectively for payloads `< 10KB`. |
| Updates queue compression metrics | Intercepts `hIncrBy` to confirm metrics are strictly tracked once per payload using byte lengths. |
| DLQ Compression | Guarantees tasks dispatched to the DLQ gracefully carry over their compressed states. |
| Prefix Spoofing Bypass Defense | Verifies that a small spoofed `__gz_json_b64__:` string is safely round-tripped and neutralized. |
| Legacy Data Morphing Protection | Guarantees that older `__gz_b64__:` compressed strings do not parse incorrectly into booleans or numbers. |

`npm test` passes completely for all strict edge-cases. `npm run type-check` passes.

Fixes #10
