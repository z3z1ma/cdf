Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-d2-duckdb-arrow-bulk.md, .10x/tickets/2026-07-11-p3-d6-compact-provenance-conformance.md

# DuckDB compact row-range provenance

## What was observed

The original inline provenance shape repeated package-hash and segment-id strings on every payload row. Its release TLC micro-path measured about 1.77M rows/s while the same DuckDB Arrow appender without provenance measured 10.4–11.3M rows/s. A compact three-column numeric tuple improved only to 3.15M rows/s while an always-maintained uniqueness index remained. Removing the hot-path index reached 8.76M rows/s. The final representation stores one allocated `UBIGINT` row key per payload row and maps contiguous segment ranges bijectively to the target, full package hash, and segment id in `_cdf_segments`; it measured 9.42M rows/s against 11.36M raw and 470k scalar rows/s (20.06x scalar).

The January TLC public HTTPS release run improved from 5.14s to 3.47s wall time. Event timestamps show package finalization remained approximately 1.70s, while DuckDB commit through receipt improved from 2.730s to 1.233s. The file contains 2,964,624 rows and 49,961,641 encoded bytes. A raw sequential download on the same host measured 0.470s at 106MB/s.

## Procedure

- `cargo test -p cdf-dest-duckdb` passed 24 active tests with one ignored performance test.
- `cargo test --release -p cdf-dest-duckdb arrow_appender_tlc_envelope_benchmark -- --ignored --nocapture` measured the before/intermediate/final shapes described above in the same harness.
- Fresh release `cdf init`, `cdf add` against the public January URL, and `cdf run tlc.yellow` loaded and checkpointed all 2,964,624 rows. The final run measured 3.47s wall, 2.14s user, and 0.29s system time.
- Addressed correction/readback, append, replace, merge, duplicate, rollback, and missing-address tests passed using the full logical tuple resolved through compact ranges.
- Raw same-file references measured 5.94ms cached sequential read, 109.9ms arrow-rs Parquet decode, and 54.7ms DuckDB Parquet count scan. These are roofline context, not equivalent end-to-end writes.

## What this supports

Long immutable identifiers must not be repeated per payload row. One allocated row key plus an exact range dimension preserves lossless logical provenance while keeping the hot path close to native append. Uniqueness is constructed by the transaction-bound allocator and verified before correction rather than maintained as an ingestion-time index.

## Limits

This closes only the DuckDB implementation slice. Postgres and Parquet adoption plus shared conformance remain owned by D6. The 3.47s end-to-end result is still far above the 0.47s network transfer; extraction/package materialization is now the largest measured gap. The DuckDB reference currently scans/counts rather than performing equivalent table creation and must be strengthened before composite-envelope claims.
