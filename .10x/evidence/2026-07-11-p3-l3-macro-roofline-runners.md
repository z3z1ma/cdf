Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-10-p3-ws-l3-macro-roofline-runners.md

# P3 L3 macro and reference runner evidence

## What was observed

`cdf-benchmarks` now exposes one typed host capability boundary and the `cdf-p3-lab` isolated worker/CLI. Macro cells retain every raw sample and derive median, median absolute deviation, rates, maximum RSS, and spill exactly; report validation rejects summaries that do not reproduce from the retained samples. Worker-reported timed regions exclude JSON/process startup while the parent independently enforces timeout and records child CPU/RSS where supported.

Warm mode executes an untimed warm-up. Cold mode re-runs explicit privileged cache eviction before every sample and becomes unavailable when the operator did not opt in. Uncontrolled mode remains separately keyed. Host-class changes become inconclusive cells. Child output is streamed and capped at 1 MiB, commands reject embedded credential-shaped environment values, and profiler/reference absence is typed report data.

Reference workers cover sequential read, sequential write with optional sync, bounded memcpy, raw Arrow Parquet/CSV/NDJSON, and DuckDB `read_parquet`. Polars remains outside Cargo: an isolated Python module probe either records its exact version and builds a scan worker or produces an unavailable cell. Flamegraph and perf wrappers record exact discovered tool/version, command, and an artifact below ignored `target/cdf-benchmarks/profiles/`.

## Procedure and results

```text
CARGO_INCREMENTAL=0 cargo test -j1 -p cdf-benchmarks --locked
CARGO_INCREMENTAL=0 cargo clippy -j1 -p cdf-benchmarks --all-targets --locked -- -D warnings
cargo fmt --all -- --check
git diff --check
target/debug/cdf-p3-lab host
target/debug/cdf-p3-lab host-class
target/debug/cdf-p3-lab profile-dry-run flamegraph fixture target/debug/cdf-p3-lab host
```

- Three provider unit tests passed: BSD/GNU RSS unit parsing, cgroup quota parsing, and portable cold-cache unavailability.
- Seven existing catalog/report/fixture tests remained green with fixed canonical hashes unchanged.
- Six macro/reference integration tests passed: isolated three-sample execution, exact summary/report validation, timeout, cold unavailability, host drift, secret rejection, Arrow/DuckDB/reference cross-checks, I/O/memcpy byte accounting, profiler plans, and external Polars supported-or-unavailable behavior.
- All benchmark targets passed Clippy with warnings denied; formatting and diff checks passed.
- No new package/version entered the supply chain. `arrow-csv` and `arrow-json` were already locked transitive dependencies; L3 only made the benchmark crate's direct use explicit.

On the observed development host, the sanitized provider emitted host class `host-class-f4bf4d1c46a93156`, BSD-time child CPU/RSS authority, APFS filesystem class, Rust 1.96.1, and a supported flamegraph 0.6.13 dry-run artifact under the ignored profile root. Perf and Polars were unavailable and remained explicit. These are provider checks, not a performance baseline or claim.

## What this supports

This supports every L3 acceptance criterion: L1-schema-only output, distinct cache modes, mandatory bias labels for references, visible non-observed cells, retained distributions, process isolation, exact setup/byte authority linkage, portable provider composition, and optional privileged/external tools.

## Limits

No large dataset was acquired or generated and no CDF throughput baseline was recorded. Linux runtime providers are covered by parser/portable behavior tests here but require L5 observation on a Linux host class. CI policy and envelope generation remain L4; baseline execution remains L5.
