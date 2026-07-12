Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-d2-duckdb-arrow-bulk.md, .10x/tickets/done/2026-07-11-p3-a5c-durable-segment-stream.md

# DuckDB Arrow streaming milestone

## What was observed

Commits `c78ae76e` and `a0c6ad1d` replace DuckDB's package-wide scalar row materialization with bounded Arrow-native ingestion. The runtime declares `SegmentStreaming`; each verified segment crosses the Arrow 59/58 C Stream bridge and DuckDB's native data-chunk appender in 64k-row batches. Framework provenance constants are added by one vectorized SQL transfer per segment rather than materialized as repeated Arrow strings. Append/replace write transactionally; merge uses a disk-backed temporary stage with SQL null-key/conflicting-duplicate checks and deterministic first-row deduplication.

The binding's vtab alternative was source-inspected and rejected because its query-parameter helper permanently retains every batch in a process-global arena. The selected C Stream bridge is governed by `.10x/decisions/duckdb-arrow-c-stream-version-bridge.md` and has compile-time layout assertions plus 32-case property coverage over length/null ownership transfer.

## Procedure

- `cargo test -p cdf-dest-duckdb` — 23 passed, one performance test ignored.
- `cargo clippy -p cdf-dest-duckdb --all-targets -- -D warnings` — passed.
- `cargo test -p cdf-project runtime_tests` — 77 passed; one stale dedup-v2 assertion failed, was corrected to use the package reader's version-neutral provenance API, and its focused rerun passed.
- `cargo test --release -p cdf-dest-duckdb arrow_appender_tlc_envelope_benchmark -- --ignored --nocapture` — passed the ≥1M rows/s envelope on a generated 19-column TLC shape.
- `/usr/bin/time -l` on the same release benchmark recorded 1,945,920 Arrow rows/s versus 438,469 rows/s for the exact removed three-stage scalar materialize/clone/append shape: 4.44x. The scalar comparison used 262,144 rows and drove the command's maximum resident set to 1,868,005,376 bytes; the new path processes fixed 65,536-row batches.

## What this supports or challenges

This supports the D2 Arrow-native, ≥1M rows/s, semantic-conformance, and bounded-memory claims. It closes DuckDB as the last first-party materialized destination for A5c.

The same-host exact removed-path comparison is 4.44x, not the ratified ≥5x. D2 therefore remains open. Further measurement/tuning must close that remaining 12.6% gap without weakening provenance uniqueness or transaction semantics.

## Limits

The benchmark is a generated TLC-shaped in-memory DuckDB workload, not the full HTTPS-to-package-to-database macro path. Single-writer blocking-lane execution still depends on A5e graph integration. Nested types remain outside the current DuckDB sheet/type map and cannot be claimed from this result.
