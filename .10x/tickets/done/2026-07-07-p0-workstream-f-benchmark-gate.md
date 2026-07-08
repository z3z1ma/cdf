Status: done
Created: 2026-07-07
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-07-p0-structural-debt-program.md

# P0 Workstream F: Open the benchmark gate

## Scope

Promote the baseline benchmark suite from triage to implementation so P4 performance work proceeds from measured baselines rather than intuition.

Expected implementation home is a dedicated, non-published benchmark workspace member such as `crates/cdf-benchmarks` with Criterion bench targets and deterministic fixture generation. If implementation evidence shows root `benches/` is materially simpler, record that rationale before editing.

## Required outcome

- Deterministic local fixtures exist for the benchmark workload envelope.
- The benchmark suite covers:
  - engine path versus direct DataFusion and direct Arrow operations;
  - file -> package per supported local format;
  - package replay per MVP destination;
  - REST decode from local fixtures;
  - package archive IPC-to-Parquet transcode;
  - startup latency for tiny runs;
  - medium and wide pipelines.
- Fair-comparison labels distinguish CDF package/receipt/checkpoint overhead from native DataFusion, DuckDB, Arrow, and Polars-style local workloads.
- The harness is wired as an opt-in quality phase with trend recording.
- First baseline numbers are recorded before any follow-on performance ticket claims a delta.
- Subsequent performance tickets cite benchmark deltas, starting with Arrow-native DuckDB bulk load, streaming commit after Workstream A, and local partition parallelism.

## Acceptance criteria

- Benchmark fixtures are generated deterministically from committed fixture specs; large generated data is not committed unless explicitly justified.
- Criterion or an equivalent ratified harness is added only to the benchmark crate or bench target that imports it.
- Runtime budget is explicit: a smoke subset suitable for local quality, and a fuller opt-in suite for weekly/deep passes.
- Metrics classify as release gate, trend-only, or ad hoc investigation before being used for closure.
- `QUALITY.md` benchmark, jscpd, rust-code-analysis, and raw size-metric phases are referenced in evidence expectations.
- The old triage-only benchmark ticket is closed, cancelled, or reparented so there is one active owner for the benchmark gate.

## Evidence expectations

Record first baseline output, fixture generation proof, command list, trend-output location, jscpd/rust-code-analysis/scc metrics for benchmark code, focused tests for fixture determinism, and adversarial review.

## Explicit exclusions

No optimization work, no public performance claim, no benchmark-baseline update without explicit evidence, no CI hard gate until baseline noise is understood, and no performance comparison that hides CDF package/receipt/checkpoint semantics.

## Progress and notes

- 2026-07-07: Opened from P0 stop-line. The prior baseline benchmark owner was triage-only; this workstream is the implementation owner.
- 2026-07-07: Cancelled the old triage-only owner at `.10x/tickets/cancelled/2026-07-07-performance-baseline-benchmark-suite.md`, so this is the single active benchmark-gate owner.
- 2026-07-08: Activated by worker for narrow benchmark-harness implementation. Implementation home remains the expected dedicated non-published workspace crate `crates/cdf-benchmarks`; no root `benches/` rationale is needed.
- 2026-07-08: Added a private workspace benchmark crate at `crates/cdf-benchmarks` with committed deterministic fixture specs, generated local data under temp directories/`target/`, Criterion smoke/full groups, a JSONL trend recorder, explicit runtime budgets, and metric classes (`release_gate`, `trend_only`, `ad_hoc`). No production optimization code or CI hard gate was added.
- 2026-07-08: Implemented executable matrix cells for direct Arrow, direct DataFusion, direct DuckDB-style insert work, CDF engine-to-package, file-to-package for CSV/JSON/NDJSON/Parquet, Arrow IPC stream-to-package, package replay to DuckDB and Parquet destinations, REST decode from local fixtures, package archive IPC-to-Parquet transcode, startup tiny run, and medium/wide pipelines. Labels distinguish CDF package/receipt/checkpoint overhead from native Arrow/DataFusion/DuckDB-style work.
- 2026-07-08: Deferred/excluded matrix cells with exact reasons: Postgres package replay requires a live Postgres database URL/service; FileResource Arrow IPC file input is rejected by the current public file runtime, so the harness uses the public Arrow IPC stream reader instead; native Polars comparison would add a heavy new non-MVP dependency that is not present elsewhere in the workspace.
- 2026-07-08: Focused command results: `cargo check -p cdf-benchmarks` initially failed because the bench crate used the DataFusion SQL API without enabling SQL features; the native DataFusion case was switched to the DataFrame API. `cargo fmt -p cdf-benchmarks` passed. `cargo check -p cdf-benchmarks --locked` passed. `cargo test -p cdf-benchmarks --locked` passed, including 3 fixture/matrix tests. `CDF_BENCH_SUITE=smoke cargo bench -p cdf-benchmarks --bench baseline --locked` passed. `crates/cdf-benchmarks/scripts/record-trend.sh smoke target/cdf-benchmarks/trends/smoke-check.jsonl` passed and wrote the smoke trend JSONL under `target/`.
- 2026-07-08: Parent review repair superseded the earlier Postgres-deferred coverage note. Postgres package replay is now `implemented_opt_in` as suite `postgres`: `CDF_BENCH_POSTGRES_URL=<postgres-url> CDF_BENCH_SUITE=postgres cargo bench -p cdf-benchmarks --bench baseline --locked`. The case uses `ResolvedProjectDestination::postgres(...)` with `PostgresTarget::new(None, "orders")`, `MergeDedupPolicy::Last`, and the existing package fixture path; normal `smoke` and `full` suites do not include it or require a service. The opt-in run mutates the disposable database named by `CDF_BENCH_POSTGRES_URL`.
- 2026-07-08: Repair command results: `cargo fmt -p cdf-benchmarks` passed. First `cargo check -p cdf-benchmarks --locked` failed because the new benchmark-only `cdf-dest-postgres` dependency edge needed a `Cargo.lock` refresh; `cargo check -p cdf-benchmarks` passed and refreshed the lockfile; rerun `cargo check -p cdf-benchmarks --locked` passed. `cargo test -p cdf-benchmarks --locked` passed, including updated matrix tests for the Postgres opt-in suite and smoke/full exclusion. `CDF_BENCH_SUITE=smoke cargo bench -p cdf-benchmarks --bench baseline --locked` passed and did not run the Postgres case.
- 2026-07-08: Second parent review repair split the benchmark harness out of monolithic `crates/cdf-benchmarks/src/lib.rs` into `catalog.rs`, `fixtures.rs`, `matrix.rs`, `resource.rs`, and `runners.rs`; `lib.rs` now only declares modules, re-exports the public surface, and owns shared error/result helpers. Public exports and behavior were preserved. Added a lightweight test assertion for `MetricClass::ReleaseGate.as_str() == "release_gate"`.
- 2026-07-08: Split repair command results: `cargo fmt -p cdf-benchmarks` passed. `cargo check -p cdf-benchmarks --locked` passed. `cargo test -p cdf-benchmarks --locked` passed, including the release-gate string assertion. Smoke bench was not rerun because this repair was a module-only refactor with no intended runtime behavior change.
- 2026-07-08: Parent verification repair fixed `clippy::cmp_owned` in `crates/cdf-benchmarks/src/bin/cdf-benchmark-trend.rs` by hoisting the default trend output `PathBuf` before comparing it. Command results: `cargo fmt -p cdf-benchmarks` passed. `cargo clippy -p cdf-benchmarks --all-targets --locked -- -D warnings` passed. `cargo test -p cdf-benchmarks --locked` passed.
- 2026-07-08: Parent security-scan repair replaced direct `std::env::args*` parsing in `crates/cdf-benchmarks/src/bin/cdf-benchmark-trend.rs` with a benchmark-bin scoped `clap` builder parser. The parser reads raw option values and explicitly rejects non-UTF-8 `--suite`/`--out` values before applying existing suite/path behavior. The first attempted `args_os()` repair still tripped Semgrep's Rust args rule, so the final implementation avoids direct `std::env::args` and `std::env::args_os` calls. Command results: `cargo fmt -p cdf-benchmarks` passed. `cargo check -p cdf-benchmarks` passed to refresh the benchmark-only `clap` dependency edge in `Cargo.lock`; a feature-trimmed `clap` dependency was then checked again successfully. `cargo clippy -p cdf-benchmarks --all-targets --locked -- -D warnings` passed. `cargo test -p cdf-benchmarks --locked` passed. `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-p0-f-benchmarks.json crates/cdf-benchmarks` passed with 0 findings.
- 2026-07-08: Parent dependency-policy repair added cargo-vet exemptions only for the Criterion/Clap benchmark dependency subtree packages reported by the parent run. Criteria match the parent report exactly: Criterion/Plotters/Rayon/Ciborium/etc. helper packages are `safe-to-run`, and Clap parser packages are `safe-to-deploy`. No audit claim was added. Command result: `cargo vet --locked > target/quality/reports/cargo-vet-p0-f-benchmarks.txt 2>&1` passed; report contains `Vetting Succeeded (424 exempted)`.
- 2026-07-08: Parent closure evidence recorded in `.10x/evidence/2026-07-08-p0-workstream-f-benchmark-gate.md`; adversarial review recorded in `.10x/reviews/2026-07-08-p0-workstream-f-benchmark-gate-review.md`. Closure verification included smoke/full/postgres benchmark suites, smoke/full/postgres trend JSONL records, fixture determinism tests, jscpd, rust-code-analysis, scc, Semgrep, gitleaks, cargo audit, cargo deny, cargo vet, OSV known-advisory confirmation, direct unsafe scan, and CodeQL through the reusable database wrapper. Workstream F is done.

## Blockers

None for the baseline harness. Streaming-commit performance work depends on Workstream A.
