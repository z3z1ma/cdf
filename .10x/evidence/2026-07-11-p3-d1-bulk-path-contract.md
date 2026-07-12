Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-d1-bulk-path-contract.md, .10x/specs/destination-bulk-path-runtime.md

# P3 D1 neutral bulk-path contract evidence

## What was observed

Commits `81281a77`, `1bfab8ca`, and `f72db7a0` establish a destination-neutral, versioned bulk-path descriptor and preparation contract in `cdf-runtime`. The bounded writer boundary accepts one accounted `CommitBatch` at a time. A mock declares two ordered paths with staging, concurrency, size ranges, and distinct fallback policies without any generic destination-name or path-id dispatch.

The attempt coordinator rejects mismatched abort proofs, non-clean staging, target visibility, and reuse of the failed load-attempt id. A successful fallback records the failed and replacement attempts, settings, and reason as validated, serializable run-event details. Physical choice remains outside package identity.

First-party DuckDB, Postgres, and Parquet runtimes publish truthful compatibility descriptors. Their existing receipt and gate semantics remain unchanged; optimized drivers are owned by D2-D4.

## Procedure

- `cargo test -p cdf-runtime` — 28 passed, 1 performance test ignored.
- `cargo clippy -p cdf-runtime -p cdf-dest-duckdb -p cdf-dest-postgres -p cdf-dest-parquet --all-targets -- -D warnings` — passed.
- `cargo test -p cdf-dest-duckdb -p cdf-dest-postgres -p cdf-dest-parquet` — DuckDB 21, Parquet 27, and Postgres 30 tests passed, including live Postgres rollback, merge, replace, duplicate, correction, and receipt cases.
- `gitleaks protect --staged --no-banner --redact` — passed before each commit.
- Source inspection confirmed `BulkWriterAttempt::write_batch` accepts `CommitBatch`, not a package, segment vector, row vector, or generic scalar-row representation.

## What this supports or challenges

This supports every D1 acceptance criterion: extension composition, bounded input, live machine-readable declarations, auditable physical attempts, proof-gated full redrive, and first-party semantic compatibility. It also supplies D2-D4 with one architectural boundary rather than destination-specific runtime wiring.

## Limits

This evidence does not claim Arrow-native DuckDB, binary Postgres COPY, streaming Parquet row-group performance, or P3 envelope attainment. Those are deliberately excluded from D1 and remain owned by D2-D5.
