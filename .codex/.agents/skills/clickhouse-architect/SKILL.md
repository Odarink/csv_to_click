---
name: clickhouse-architect
description: Use when designing, reviewing, or tuning ClickHouse schemas, MergeTree tables, sort or partition keys, codecs, projections, materialized views, dictionaries, ingestion idempotency, or slow ClickHouse queries.
---

# ClickHouse Architect

Design and review ClickHouse storage and query behavior from the actual workload, table definition, and available evidence.

## Workflow

1. Inspect the existing DDL, representative queries, data volume, retention requirements, ingestion pattern, ClickHouse version, and deployment type when available.
2. Separate correctness risks from performance opportunities: duplication, replay behavior, mutation races, late data, partition replacement, and result semantics come first.
3. Review `ORDER BY` / primary key, partitioning, column types and codecs, engine, TTL, projections, materialized views, and dictionaries only as justified by the workload.
4. Ask for missing business constraints or production evidence when a consequential recommendation cannot be derived from repository artifacts.
5. Present the proposed DDL or SQL change, reasons, expected tradeoffs, and validation queries or measurements.

## References

- Read [references/schema-design.md](references/schema-design.md) when choosing table layout, keys, types, codecs, or accelerators.
- Read [references/operations-and-performance.md](references/operations-and-performance.md) when investigating load patterns, mutations, query performance, or operational validation.

## Guardrails

- Do not claim a speedup or ideal key order without workload or benchmark evidence.
- Treat `PARTITION BY` primarily as lifecycle and maintenance design; do not use high-cardinality partitioning casually.
- Prefer idempotent replacement or partition-scoped reload strategies for pipelines; explain any delete-and-insert race risk.
- Preserve existing business logic unless the task explicitly requests a behavioral change.
- Identify assumptions when DDL, cardinality, query logs, or row distributions are unavailable.

## Output

Lead with concrete correctness or performance findings. Then give the proposed table/query changes, validation steps (`EXPLAIN`, `system.*` inspection, duplicate checks, timing or bytes-read comparison), and any remaining assumptions.
