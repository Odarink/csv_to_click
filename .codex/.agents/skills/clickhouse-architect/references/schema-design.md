# ClickHouse Schema Design

Use this reference when a task requires physical table design or DDL review.

## Inputs To Establish

- Query predicates, grouping dimensions, sort/range access patterns, freshness and retention needs.
- Data volume, insert batch shape, cardinalities, late-arriving or corrected records.
- Deployment and engine constraints: `MergeTree`, replicated deployment, or Cloud-managed engine behavior.

## Keys And Partitioning

- Select `ORDER BY` for the frequent selective access paths and locality required by aggregations.
- Put columns used in common filtering/range scans early when their ordering helps prune data; validate ordering against real query shapes and cardinalities.
- Keep a custom `PRIMARY KEY` compatible with `ORDER BY`; use it only when a shorter sparse index is justified.
- Choose `PARTITION BY` for retention, detach/drop, backfill, and replacement operations. Monthly time partitions are often a usable starting point, but validate part counts and reload needs.
- Avoid high-cardinality partitions and excessive tiny partitions.

## Types And Codecs

- Use the smallest semantically correct numeric and temporal types.
- Consider `LowCardinality(String)` for repeatedly occurring categorical strings after measuring suitability.
- Treat codecs as data-pattern decisions: delta-family codecs can help ordered temporal or integer series; Gorilla may help floating measurements; LZ4 versus ZSTD is a read-speed versus compression tradeoff.
- Measure compressed bytes and read latency before standardizing a codec choice.

## Accelerators

- Use projections for recurring alternative access order or pre-aggregation only after confirming the baseline query pattern.
- Use materialized views for stable derived aggregates with explicit refresh/backfill semantics.
- Use dictionaries when dimension lookup semantics and refresh behavior are clear; do not replace joins reflexively.

## DDL Review Checklist

- Engine and replication/deployment choice are appropriate.
- Keys support actual filtering and maintenance.
- Partition count and part size are manageable.
- TTL, reload, correction, and deduplication behavior are explicit.
- Table and non-obvious derived columns receive useful comments when DDL is changed.
