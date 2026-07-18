Status: recorded
Created: 2026-07-11
Updated: 2026-07-17
Relates-To: .10x/tickets/done/2026-07-11-p3-v2-validation-graph-integration.md, .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md

# V2 smoke macro profile after vector integration

## What was observed

The complete `cdf_baseline_smoke` Criterion suite executed after the package fixture was brought onto the current package contract by writing its runtime Arrow schema. The engine package/filter/project workload measured 128.62–133.44 ms with a 131.11 ms median. Criterion reported a statistically significant median improvement of 7.7843% against the locally stored comparable baseline (`p = 0.01`).

Other cells did not establish a defensible change: NDJSON file-to-package measured 137.99–182.42 ms, REST 2.6928–2.7980 ms, archive IPC-to-Parquet 114.78–183.68 ms, and DuckDB package replay 193.17–301.31 ms. Criterion classified each as no detected change because its confidence interval crossed zero.

## Procedure

```text
CDF_BENCH_SUITE=smoke cargo bench -p cdf-benchmarks --bench baseline --locked
cargo test -p cdf-benchmarks --locked
cargo test -p cdf-conformance golden_package --locked
```

The smoke benchmark completed successfully. Benchmark-crate unit and fixture tests passed until the pre-existing `preoptimization_baseline_covers_every_target_and_retains_phases` assertion required the intentionally legacy `legacy_tiny_startup_e2e` import to be observed. That legacy expectation belongs to `.10x/tickets/done/2026-07-11-p0-remove-preproduction-compatibility-vestiges.md`; it is not evidence against the current package fixture.

The conformance build did not reach golden execution because active destination-extension work has already removed concrete `ResolvedProjectDestination::{duckdb,parquet_filesystem,postgres}` constructors while `cdf-conformance` still references them. This integration drift is owned by `.10x/tickets/done/2026-07-11-p0-dx2-driver-owned-adapters-composition.md` and `.10x/tickets/done/2026-07-11-p0-dx3-generic-lock-doctor-replay.md`.

## What this supports or challenges

This supports a macro-level package/engine improvement after vector validation integration and proves the current DuckDB replay fixture can be constructed and benchmarked. It challenges any claim that all neighboring smoke cells improved; their samples are too noisy.

## Limits

The smoke fixture is not the full TLC roofline, a constant-memory stress run, an RSS profile, or final golden-package evidence. The local Criterion baseline is comparable only within this checkout and host history. V2 cannot close until the conformance composition migration is coherent and the remaining golden/RSS criteria execute.
