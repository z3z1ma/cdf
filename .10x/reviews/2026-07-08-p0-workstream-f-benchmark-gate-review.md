Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-07-p0-workstream-f-benchmark-gate.md
Verdict: pass

# P0 Workstream F benchmark gate review

## Target

P0 Workstream F implementation: private `crates/cdf-benchmarks` harness, fixture specs, Criterion suites, JSONL trend recorder, scoped dependency policy, and closure records.

## Assumptions tested

- The benchmark gate must not become a public performance claim.
- The harness must cover the required workload envelope without adding production optimizations.
- Package replay must include DuckDB, Parquet, and Postgres.
- The user preference against monolithic crate roots applies to the new benchmark crate.
- Quality evidence must include benchmark output, jscpd, Rust metrics, raw size metrics, supply-chain checks, security scans, and CodeQL through the reusable database wrapper.

## Findings

No blocking findings.

Minor residual risk: the benchmark crate adds a dev/performance-only dependency subtree for Criterion and a small `clap` parser for the trend recorder. Cargo-vet exemptions were added for exact versions and criteria, but they are exemptions rather than audits. This matches the existing project policy style and keeps future dependency movement visible.

Minor residual risk: the Postgres benchmark requires a disposable live database and is therefore opt-in through `CDF_BENCH_POSTGRES_URL`. Parent evidence did run it against local `initdb`/`pg_ctl`, proving the cell is executable in this environment.

Minor residual risk: CodeQL reports the known Rust extractor macro-warning pattern. The SARIF contains 0 query results, and this limitation is already documented in `.10x/knowledge/quality-gate-execution.md`.

## Verdict

Pass. The Workstream F acceptance criteria are supported by implementation and evidence. The harness is opt-in, locally reproducible, fair-labeled, non-publication-oriented, and wired to trend output. No production optimization or CI hard gate was added.

## Residual risk

Future performance tickets must cite these baseline outputs or refresh them before claiming deltas. The benchmark gate does not by itself implement performance optimizations, memory accounting, byte-bounded backpressure, or partition parallelism.
