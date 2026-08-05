# Agent rules

## Code map stays in sync with the repo

At the start of every code-changing task, compare the current repo with docs/codemap/codemap.lock.

Before modifying a module, use docs/codemap/codemap.json to answer three questions:

1. What calls it?
2. What does it affect?
3. Which tests cover it?

If the map is stale or cannot answer those questions, regenerate codemap.html, codemap.json, and codemap.lock before changing the code.

Whenever module boundaries, dependencies, routes, databases, queues, or major data flows change, update the code map in the same commit as the code.

docs/codemap/codemap.html, codemap.json, and codemap.lock are always generated together from the current repo. Never edit only one of them manually.
