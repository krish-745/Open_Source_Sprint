# Phase 2: Redis Denial of Service (DoS) Hunt

This directory contains a highly customized Semgrep rule designed specifically for the Taskflow architecture.

## The Attack Vector
Taskflow uses Redis Lists (`lPush`) and Sorted Sets (`zAdd`) to manage task queues. If the `createTask` API allows a user to push tasks endlessly without the backend ever checking `lLen` or `zCard` to enforce a maximum queue depth, an attacker can write a simple `while(true)` loop to spam the API. 

This will exhaust the Redis server's RAM, causing the Redis process to crash (OOM Kill), taking down the entire distributed task system. This is a severe Denial of Service (DoS) 0-day.

## How to run the scan

Execute this command in your terminal from the root of the project:

```bash
semgrep --config custom_tests/0day-redis-dos/unbounded-redis.yml src/
```

If Semgrep finds any matches, it means there are insertion paths completely missing bounds checks. Copy the output back to me so we can trace if those lines are reachable via the public API!
