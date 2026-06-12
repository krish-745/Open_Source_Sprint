# Issue Sprint Workflow

## Before Starting Any Issue

Scan open AND recently merged PRs. If the issue is already claimed or merged, skip it and pick the next one.

---

## Folder Structure

Each issue gets its own isolated folder inside `custom_tests/`:

```
custom_tests/
├── issue18/
│   ├── regression.test.ts   ← fails BEFORE fix, passes AFTER
│   ├── suite.test.ts        ← full test suite (committed to src/)
│   └── pr.md                ← PR title + body, ready to paste
├── issue2/
│   ├── regression.test.ts
│   ├── suite.test.ts
│   └── pr.md
└── workflow.md              ← this file
```

All `custom_tests/` contents are gitignored. Only the final test suite gets committed to `src/services/__tests__/`.

---

## Roles

| Who | Does What |
|---|---|
| **Agent (writer)** | Reads the issue, writes regression test, writes fix, writes full suite, writes PR markdown |
| **You (executor)** | Runs tests in terminal, confirms output, runs type-check, commits, pushes, opens PR |

The agent never runs tests. You are the single source of truth on whether something passes or fails.

---

## Step-by-Step Process

### Step 1 — Pick an Issue
- Agent checks the PR page and `GITHUB_ISSUES.md`
- Agent picks the best available issue (no open PR for it yet)
- Agent announces: issue number, difficulty, what it fixes

---

### Step 2 — Agent Writes the Regression Test
Agent creates:
```
custom_tests/issueN/regression.test.ts
```
This test is written to **intentionally fail on the current unmodified code**, proving the bug exists.

**You run:**
```bash
npx jest custom_tests/issueN/regression.test.ts --roots="<rootDir>"
```

✅ Expected: **FAIL** — confirms the bug is real.
If it passes, the bug doesn't exist or the test is wrong. Tell the agent.

---

### Step 3 — Agent Writes the Fix
Agent modifies the relevant file(s) in `src/`.

**You run the regression test again:**
```bash
npx jest custom_tests/issueN/regression.test.ts --roots="<rootDir>"
```

✅ Expected: **PASS** — fix is working.
If it still fails, tell the agent. Agent investigates and iterates.

---

### Step 4 — Agent Writes the Full Test Suite
Agent creates the committed test file:
```
src/services/__tests__/<service>.test.ts
```
This contains clean, complete tests (no debugging artifacts).

**You run the full suite:**
```bash
npm test
```

✅ Expected: **All tests PASS**.

> **Note on Testing Rigor:** Make sure to do multiple checks on test tasks to ensure absolutely nothing gets away. Double-check edge cases and verify stability across multiple runs if necessary.

---

### Step 5 — Type Check
**You run:**
```bash
npm run type-check
```

✅ Expected: **No errors**.
If there are errors, tell the agent the exact output.

---

### Step 6 — Agent Writes the PR Markdown
Agent creates:
```
custom_tests/issueN/pr.md
```
Formatted and ready to paste directly into GitHub. No personal paths, no informal language, no hardcoded line numbers.

---

### Step 7 — You Commit and Push
**Strict Branch Requirements:**
- You MUST create a new branch for each issue.
- You MUST branch directly off of the main/master branch (never branch off another feature branch).
- The branch name MUST follow the format: `fix/<N>-short-description` or `feat/<N>-short-description`.

```bash
git checkout main                     # Ensure you start from clean main
git pull                              # Get latest updates
git checkout -b fix/<N>-short-description
git add src/                          # only src/ changes
git commit -m "fix(<scope>): <description> (#N)"
git push -u origin fix/<N>-short-description
```

Open the PR on GitHub using the content from `custom_tests/issueN/pr.md`.

---

### Step 8 — Check the PR Page Again
Before moving to the next issue, reload:
> https://github.com/krish-745/Open_Source_Sprint/pulls

Make sure your PR is visible and correctly targeting `krish-745/Open_Source_Sprint`. Then pick the next issue and repeat from Step 1.

---

## Ground Rules

- **One issue at a time.** Never mix fixes across issues in the same branch.
- **Strict Branching.** Always branch from the clean main/master branch.
- **Server Reloading.** If you are testing changes against a live server, CANCEL your current `npm run dev` process (Ctrl+C) and REDO `npm run dev` to ensure your newest code changes are loaded into memory.
- **Test Mock Maintenance (Crucial Lesson Learned).** This project relies on hand-rolled Redis mocks (like `makeFakeRedis` or `makeClient`) instead of a robust library. When introducing new Redis commands (e.g., `eval` for Lua scripts, `zCard`, `hGet`), you **must** proactively update the mocks in the test files to prevent breaking existing tests or causing integration failures when other PRs merge.
- **Agent proposes, you approve.** Agent writes everything; you decide when to run and commit.
- **Regression test must fail first.** If it doesn't fail on unmodified code, the test is not valid.
- **Only `src/` gets committed.** `custom_tests/` stays local always.
- **Check the PR page before and after each issue.** The sprint is live; others are submitting too.
