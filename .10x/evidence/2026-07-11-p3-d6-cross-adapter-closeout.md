Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-d6-compact-provenance-conformance.md

# Compact provenance cross-adapter closeout

## What was observed

The conformance destination catalog now inspects every enrolled first-party driver and requires supported row-provenance persistence and targetability on the shared kernel contract. The law has no destination-name match arm; enrolling another destination subjects its sheet to the same assertion.

DuckDB and PostgreSQL persist one allocated row key per payload row plus exact transactionally bound segment ranges. Parquet persists no row provenance column; its receipt-bound object/provenance manifest resolves exact object key and row ordinal. Every adapter consumes and exposes the same `(package hash, segment id, row ordinal)` logical address.

The current adapter suites passed 24 DuckDB, 30 PostgreSQL, and 27 Parquet non-ignored tests. Strict all-target Clippy passed across conformance and all three destination crates. Performance evidence is 9.76M DuckDB rows/s (84.6% raw appender), 1.60M PostgreSQL server-inclusive rows/s (2.64x CSV), and 0.642x raw-write Parquet throughput with no per-row provenance payload.

## Procedure

```text
cargo test -p cdf-conformance destination_catalog --locked
cargo test -p cdf-dest-duckdb --locked
cargo test -p cdf-dest-postgres --locked
cargo test -p cdf-dest-parquet --locked
cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings
cargo clippy -p cdf-dest-duckdb -p cdf-dest-postgres -p cdf-dest-parquet --all-targets --locked -- -D warnings
```

## What this supports or challenges

This supports homogeneous logical provenance with adapter-owned optimal physical layouts and no generic destination branch. It challenges literal physical-layout homogeneity as both slower and architecturally leaky.

## Limits

The aggregate catalog law validates declarations while detailed runtime round trips remain in adapter suites. D5 owns the fourth-driver/full envelope enrollment law and generated matrix presentation.
