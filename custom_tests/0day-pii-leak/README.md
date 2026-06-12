# Phase 3: Information Disclosure via Logging (PII Leaks)

This directory contains a custom Semgrep rule to hunt for Information Disclosure 0-days.

## The Attack Vector
When a task fails, backend workers often log the failure for debugging. If they log the entire raw `task` object, `payload`, or `metadata` without sanitizing it first, any sensitive data provided by the user (like API Keys, OAuth tokens, passwords, or PII) gets dumped straight into plain-text logs. This violates SOC2 and GDPR compliance.

We checked `GITHUB_ISSUES.md` and this flaw is **completely unregistered**. If we find it, it's a guaranteed 0-day.

## How to run the scan

Execute this command in your terminal from the root of the project:

```bash
semgrep --config custom_tests/0day-pii-leak/pii-leak.yml src/
```

If it spits out any matches, we have found our 0-day!
