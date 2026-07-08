# CDF Baseline Benchmarks

This crate is a private, opt-in benchmark harness. It does not publish performance claims and does not add CI hard gates.

Committed fixtures live in `fixtures/baseline-fixtures.json` as deterministic specs only. Generated CSV, JSON, NDJSON, Arrow IPC, Parquet, packages, DuckDB files, and trend output are created under temp directories or `target/`.

## Commands

Smoke Criterion pass, intended local budget under 2 minutes:

```bash
CDF_BENCH_SUITE=smoke cargo bench -p cdf-benchmarks --bench baseline --locked
```

Full opt-in Criterion pass, intended deep/weekly budget under 15 minutes:

```bash
CDF_BENCH_SUITE=full cargo bench -p cdf-benchmarks --bench baseline --locked
```

Postgres package replay is a separate opt-in suite because it requires and mutates a live disposable database. It replays the package fixture to target table `orders` through `ResolvedProjectDestination::postgres(...)` with `MergeDedupPolicy::Last`:

```bash
CDF_BENCH_POSTGRES_URL=postgres://localhost/cdf_bench CDF_BENCH_SUITE=postgres cargo bench -p cdf-benchmarks --bench baseline --locked
```

Trend recording writes JSONL under `target/cdf-benchmarks/trends/`:

```bash
crates/cdf-benchmarks/scripts/record-trend.sh smoke
crates/cdf-benchmarks/scripts/record-trend.sh full
CDF_BENCH_POSTGRES_URL=postgres://localhost/cdf_bench crates/cdf-benchmarks/scripts/record-trend.sh postgres
```

## Metric Classes

`release_gate`: fixture-spec parsing, deterministic fixture generation, and smoke workload success. These are correctness gates, not timing thresholds.

`trend_only`: CDF elapsed-time measurements for package, receipt, checkpoint, archive, REST decode, and startup paths. Use deltas as local trend evidence only.

`ad_hoc`: native Arrow, DataFusion, and DuckDB-style local comparisons. These labels are investigation aids and do not represent equivalent CDF semantics.

Before closing benchmark-gate work from these metrics, also collect the relevant `QUALITY.md` gradient/report phases for benchmark code: Criterion/cargo bench output, `jscpd`, `rust-code-analysis-cli`, and raw `scc` or `tokei` size metrics if available.

## Coverage Notes

Implemented cells:

- CDF engine package path versus direct Arrow and direct DataFusion local work.
- File-to-package for CSV, JSON, NDJSON, and Parquet local file sources.
- Arrow IPC stream-to-package through the public stream reader.
- Package replay to local DuckDB and filesystem Parquet destinations with package receipt/checkpoint semantics.
- Package replay to Postgres is implemented as the `postgres` suite and is gated by `CDF_BENCH_POSTGRES_URL`, so normal smoke/full runs do not require a service.
- REST decode from local fixture responses through an in-memory transport.
- Package archive IPC-to-Parquet transcode.
- Tiny startup, medium, and wide pipeline envelopes.
- Native DuckDB-style local insert as an `ad_hoc` label.

Excluded/deferred cells:

- FileResource Arrow IPC file input is excluded because the public file runtime rejects declarative `arrow_ipc`; the harness uses the public Arrow IPC stream reader instead.
- Native Polars comparison is excluded to avoid adding a heavy new dependency for a non-MVP comparison label.
