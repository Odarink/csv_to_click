---
name: code-review
description: Use when reviewing a pull request, branch diff, local code changes, implementation proposal, or pre-merge state for correctness bugs, regressions, security concerns, performance risks, and missing test coverage.
---

# Code Review

Review like an owner: identify behavior that can break, mislead users, expose data, or remain untested.

## Review Procedure

1. Establish intended behavior from the request, issue, diff context, or tests.
2. Inspect changed paths and enough surrounding code to trace real behavior and contracts.
3. Look first for correctness, data-loss, security, compatibility, concurrency, and error-handling issues.
4. Check performance and maintainability when they create material operational risk.
5. Evaluate whether tests cover changed behavior and relevant failure cases.
6. Report only defensible findings, with precise file/line references and a practical fix direction.

## Finding Levels

- `blocker`: security, data-loss, or severe correctness issue that must not ship.
- `issue`: concrete behavioral defect or material regression that should be fixed before merge.
- `suggestion`: worthwhile improvement without a demonstrated shipping defect.
- `question`: required clarification where behavior cannot be established from evidence.

Do not report style preferences as defects.

## Checks

- Correctness: null/empty inputs, boundary dates, duplicate handling, retries, incomplete data, error propagation.
- Design: existing contracts, unnecessary scope expansion, unjustified abstractions, backwards compatibility.
- Security: secrets, injection, authorization, destructive operations, sensitive output.
- Performance: accidental full scans, unbounded memory, N+1 work, expensive loops or joins on expected volume.
- Tests: new behavior, failure paths, regression coverage, test assertions that verify meaningful outcomes.

## Output Format

Lead with findings ordered by severity. Format each as:

```text
[level] path/to/file:line - Short title
Why this is a problem and the conditions that trigger it.
Suggested correction or verification.
```

After findings, state open questions or assumptions, then a brief summary. If there are no findings, say so and note remaining test or runtime-verification gaps.
