# ClickHouse Operations And Performance

Use this reference for existing-table investigations and pipeline behavior.

## Investigation Order

1. Confirm result correctness and idempotency requirements.
2. Capture DDL and relevant settings with `SHOW CREATE TABLE`.
3. Identify expensive queries with available logs or supplied SQL; compare bytes read, rows read, memory, and elapsed time.
4. Inspect partition and part health through `system.parts` or analogous available metadata.
5. Propose the smallest change and specify before/after measurement.

## Pipeline Risks

- Replayed loads may duplicate rows unless the table strategy or load logic is explicitly idempotent.
- Broad mutation-based deletion before an insert can be slow and can introduce operational races; prefer partition-level replacement when the business grain supports it.
- Late-arriving records and corrections require a defined overwrite, deduplication, or versioning strategy.
- Small frequent inserts create parts pressure; recommend batching only after establishing ingestion constraints.

## Useful Validation Questions

- Does the query return duplicate business keys after a rerun?
- Do affected partitions contain expected date ranges and row counts?
- Does `EXPLAIN` or read-stat evidence show key or projection use?
- Did bytes read, memory, and elapsed time improve on a representative query?
- Does a backfill or retry preserve the expected final state?

## Reporting

Distinguish verified observations from recommended experiments. When live access is unavailable, state that findings are static or artifact-backed rather than measured in production.
