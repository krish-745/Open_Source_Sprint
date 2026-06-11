# ADR 0002: Redis Data Structures Chosen

## Status
Accepted

## Context
TaskFlow needs to persist and manage tasks, worker registrations, queues, metrics, and logs. As an in-memory database, we must use the most space-efficient and operationally correct Redis data structures to ensure O(1) or O(log N) lookup times.

## Decision
We utilize the following Redis data structures:
1.  **Task & Worker Entities (Strings)**:
    *   Keys: `task:<taskId>` and `worker:<workerId>`
    *   Structure: Serialized JSON strings. Allows easy retrieval and complete updates of complex data models.
2.  **Global Indices (Sorted Sets)**:
    *   Keys: `tasks:index` and `workers:index`
    *   Scores: Timestamps. Used for sorting, pagination, and range queries (e.g. finding stale workers).
3.  **Active Priority Queues (Sorted Sets)**:
    *   Keys: `queue:<queueName>`
    *   Scores: Priority scores (`critical` = 1000, `high` = 100, `medium` = 10, `low` = 1) plus a random tie-breaker. Allows priority-based task dispatching.
4.  **Queue Statistics (Hashes)**:
    *   Keys: `queue:<queueName>:stats`
    *   Fields: `pending`, `processing`, `completed`, `failed`. Uses atomic increments (`HINCRBY`) to keep counters fast and accurate.
5.  **Worker Handler Maps (Sets)**:
    *   Keys: `worker:handlers:map:<handlerName>`
    *   Values: List of worker IDs. Enables fast lookup of workers registered to run specific tasks.
6.  **Dead Letter Queue (Lists)**:
    *   Key: `dlq:tasks`
    *   Structure: Simple queue using `LPUSH` for failed task retention.

## Consequences
*   **Pros**:
    *   Atomic queue management using sorted set operations.
    *   Low overhead updates for counters via Hashes.
    *   O(1) access to individual task details.
*   **Cons**:
    *   Updating JSON strings requires a read-modify-write cycle, introducing race condition risks if not handled with transactions.
