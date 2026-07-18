# CDF Baseline Benchmarks

This crate is the private P3 performance lab. Raw reports are evidence inputs, not performance claims; CI comparison policy is owned separately from workload execution.

Committed fixtures live in `fixtures/baseline-fixtures.json` as deterministic specs only. Generated CSV, JSON, NDJSON, Arrow IPC, Parquet, packages, DuckDB files, and trend output are created under temp directories or `target/`.

## Commands

Build the isolated macro/reference worker and print a sanitized host fingerprint:

```bash
cargo build -p cdf-benchmarks --bin cdf-p3-lab --release --locked
target/release/cdf-p3-lab host
```

## Dedicated EC2 benchmark host

P3 promotion measurements should run on a reusable EC2 benchmark host rather than a developer laptop. The helper keeps setup explicit:

```bash
tools/p3-ec2-benchmark-host.sh plan
tools/p3-ec2-benchmark-host.sh provision
tools/p3-ec2-benchmark-host.sh bootstrap
tools/p3-ec2-benchmark-host.sh sync-repo
CDF_BENCH_WORKSPACE=/path/to/cdf-workspace tools/p3-ec2-benchmark-host.sh sync-workspace
tools/p3-ec2-benchmark-host.sh build
tools/p3-ec2-benchmark-host.sh verify
tools/p3-ec2-benchmark-host.sh cdf -- run tlc.yellow --progress never
tools/p3-ec2-benchmark-host.sh lab -- host
tools/p3-ec2-benchmark-host.sh teardown
```

The instance is reused for a benchmark tranche and then explicitly terminated. Repo synchronization honors `.gitignore` and excludes `target/`, local environment files, and common secret directories. Workspace synchronization defaults to a minimal control-plane manifest (`cdf.toml`, `cdf.lock`, `resources/`, `data/`, `.cdf/state.db`, `.cdf/schemas/`, and schema-observation cache entries) so nested generated benchmark directories and destination files do not leak into the benchmark host; set `CDF_BENCH_WORKSPACE_SYNC_MODE=full` for an ignore-filtered full tree that still excludes local secrets plus generated DuckDB/package/spool artifacts. The on-host build uses the workspace release profile (`opt-level=3`, fat LTO, one codegen unit, stripped symbols) from the synchronized `Cargo.lock`.

`run-cell REQUEST.json` executes a schema-versioned macro cell with median-of-N sampling, timeout, explicit warm/cold/uncontrolled mode, child-process wall/CPU/RSS observation, reference identity, and bias labels. `reference-worker REQUEST.json` is the isolated worker for sequential read/write, memcpy, Arrow Parquet/CSV/NDJSON, direct Arrow Parquet rewrite with explicit writer policy, and DuckDB Parquet references.

Profiling plans record the exact detected tool/version, command, and ignored artifact path without requiring the tool in ordinary tests:

```bash
target/release/cdf-p3-lab profile-dry-run flamegraph cdf-package target/release/cdf --help
target/release/cdf-p3-lab profile-dry-run perf-stat cdf-package target/release/cdf --help
```

Missing tools and non-opted-in privileged cold-cache control produce typed unavailable cells; they are never omitted or simulated.

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

Unavailable/deferred cells remain explicit report rows:

- Arrow IPC file and stream framing are benchmarked separately; file framing uses the registered native driver while subprocess framing retains the stream reader.
- Polars remains an externally isolated optional reference so it does not enter CDF's Cargo graph; missing executables produce an unavailable cell.
