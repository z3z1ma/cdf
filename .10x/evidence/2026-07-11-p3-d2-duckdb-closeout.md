Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-d2-duckdb-arrow-bulk.md

# DuckDB Arrow-native bulk closeout

## What was observed

The release TLC-shaped appender benchmark measured 9,759,287 rows/s through CDF's current Arrow-native ingress, 11,534,448 rows/s through the raw binding control, and 416,802 rows/s through the exact removed scalar shape. CDF reaches 84.6% of the native control and is 23.41x faster than scalar ingestion.

The full DuckDB suite passed 24 non-ignored tests covering Arrow C Stream lengths/nulls, Decimal128/list/struct persistence, append/replace/merge, duplicate idempotency, correction, abort, rollback, compact provenance uniqueness, receipts, verification, and single-writer exclusion. Strict all-target Clippy passed.

## Procedure

```text
cargo test -p cdf-dest-duckdb --locked
cargo clippy -p cdf-dest-duckdb --all-targets --locked -- -D warnings
cargo test -p cdf-dest-duckdb --release arrow_appender_tlc_envelope_benchmark --locked -- --ignored --nocapture
```

## What this supports or challenges

This supports both D2 throughput thresholds, declared single-writer confinement, no scalar fallback for the envelope schema, and current semantic parity. Prior staged-ingress evidence separately demonstrates upstream package/destination overlap and shared byte admission.

## Limits

The benchmark is local DuckDB on this host, not the remote TLC end-to-end envelope. Decimal256 remains truthfully unsupported because the pinned DuckDB binding maps it lossily; this is a capability declaration, not scalar fallback.
