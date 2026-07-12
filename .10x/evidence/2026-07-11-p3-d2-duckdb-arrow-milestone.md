Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-d2-duckdb-arrow-bulk.md, .10x/tickets/done/2026-07-11-p3-a5c-durable-segment-stream.md

# DuckDB Arrow streaming milestone

## What was observed

Commits `c78ae76e` and `a0c6ad1d` replace DuckDB's package-wide scalar row materialization with bounded Arrow-native ingestion. The runtime declares `SegmentStreaming`; each verified segment crosses the Arrow 59/58 C Stream bridge and DuckDB's native data-chunk appender in 64k-row batches. Append/replace now retain one compact package-wide Arrow ingress plus a three-scalar row per segment range table, then execute one vectorized provenance join at finalize. This avoids repeated package/segment strings, package-sized resident metadata, and per-segment full-column SQL copies while preserving exact `(load, segment, row)` addresses. Merge retains its disk-backed temporary stage with SQL null-key/conflicting-duplicate checks and deterministic first-row deduplication.

The binding's vtab alternative was source-inspected and rejected because its query-parameter helper permanently retains every batch in a process-global arena. The selected C Stream bridge is governed by `.10x/decisions/duckdb-arrow-c-stream-version-bridge.md` and has compile-time layout assertions plus 32-case property coverage over length/null ownership transfer.

## Procedure

- `cargo test -p cdf-dest-duckdb --lib --no-fail-fast` — 24 passed, one performance test ignored.
- `cargo clippy -p cdf-dest-duckdb --all-targets -- -D warnings` — passed.
- `cargo test -p cdf-project runtime_tests` — 77 passed; one stale dedup-v2 assertion failed, was corrected to use the package reader's version-neutral provenance API, and its focused rerun passed.
- `cargo test --release -p cdf-dest-duckdb arrow_appender_tlc_envelope_benchmark -- --ignored --nocapture` — passed the ≥1M rows/s envelope on a generated 19-column TLC shape.
- The final equal-work release run processed 1,048,576 rows on both sides: 1,938,221 rows/s for the exact production-shaped Arrow/provenance path versus 332,062 rows/s for the removed scalar materialize/clone/append path, a 5.84x speedup. The raw Arrow appender control reached 11,067,972 rows/s. `/usr/bin/time -l` recorded 4,497,227,776 maximum resident bytes for the command because the removed baseline deliberately materializes the full million-row package; the production path itself remains fixed at 65,536-row batches.

## What this supports or challenges

This supports the D2 Arrow-native, ≥1M rows/s, ≥5x baseline, semantic-conformance, and bounded-production-memory claims. It closes DuckDB as the last first-party materialized destination for A5c and shows that compact exact provenance does not require repeated Arrow string columns.

## Limits

The benchmark is a generated TLC-shaped in-memory DuckDB workload, not the full HTTPS-to-package-to-database macro path. The direct 11.1M rows/s control is a kernel roofline rather than an end-to-end claim. DuckDB's finalized-package session still needs to execute through its declared blocking lane before D2 can close; nested type ingestion is covered separately by the live appender/type-matrix tests.
