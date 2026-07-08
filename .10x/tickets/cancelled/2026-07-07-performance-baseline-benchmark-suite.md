Status: cancelled
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-performance-investigation-backlog.md

# Triage baseline performance benchmark suite

## Scope

Investigate whether CDF needs a stable baseline benchmark suite before making performance claims or optimization decisions. The suite should, if ratified later, measure CDF's actual workload envelope against itself and against selected inspirations without turning every performance question into an anecdote.

This ticket is triage only. It does not authorize adding Criterion benches, CI jobs, large fixtures, generated datasets, dependency changes, or performance dashboards. It should first decide what measurements are worth owning and which comparisons are fair enough to maintain.

## Investigation questions

- What are the first benchmark scenarios that correspond to CDF's actual product promise rather than generic database shootouts?
- Which scenarios can be measured with deterministic local fixtures and no network dependence?
- Which comparisons are useful: direct Arrow Rust, DataFusion query, DuckDB local query/load, Polars lazy query, Python row ETL, Airbyte/Singer adapter style, package replay, destination commit?
- Which metrics matter per scenario: rows/sec, bytes/sec, end-to-end wall time, CPU time, peak RSS, package bytes, destination bytes, startup latency, per-batch overhead, checkpoint/receipt overhead, or retry/pagination overhead?
- What is the smallest recurring benchmark suite that would catch regressions without making normal quality checks painfully slow?

## Candidate workload matrix

- In-memory Arrow batch filtering/projection through `cdf-engine` versus direct DataFusion and direct Arrow operations.
- File source to package for CSV, JSON, Arrow IPC, and native Parquet where supported.
- Package replay into DuckDB, Postgres, and Parquet/object-store destinations using fixed package fixtures.
- Declarative REST decode from deterministic local response fixtures, not live network.
- Package archive IPC-to-Parquet transcode for representative primitive schemas.
- Small pipeline startup latency: parse project, compile resource, plan, execute one tiny package, commit, checkpoint.
- Medium batch pipeline: enough rows to amortize startup and expose IO/hash/write costs.
- Wide schema pipeline: enough columns to expose schema, projection, and coercion behavior.

## Fair comparison guidance

- DuckDB comparison should include both direct native DuckDB paths and CDF-to-DuckDB paths, labeled separately.
- DataFusion comparison should separate DataFusion's query execution from CDF's package and commit protocol overhead.
- Polars comparison should be limited to local dataframe-style workloads; it is not a replacement for CDF's package/receipt/checkpoint contract.
- Airbyte/Singer comparison should focus on adapter-style extraction and row-to-batch conversion overhead, not destination ecosystem breadth.
- Spark/Flink comparison should be qualitative unless a distributed benchmark is separately ratified.

## Acceptance criteria

- Produce a recommendation record that classifies candidate benchmarks as `must-have`, `useful later`, or `not worth maintaining`.
- Identify fixture sizes, schemas, and deterministic data-generation strategy for each recommended benchmark.
- Identify required tools and whether they already exist in `QUALITY.md` or local dependencies.
- Identify which metrics should become release gates, trend-only reports, or ad hoc investigation tools.
- If a durable benchmark harness is recommended, open a separate implementation ticket with explicit files, dependencies, acceptance criteria, and runtime budget.
- If no harness is recommended yet, record the no-action rationale and the trigger that should reopen the question.

## Evidence expectations

- Current code-path inspection for `cdf-engine`, `cdf-package`, destination package loaders, and format readers.
- At least one lightweight manual measurement or profiling sketch if the ticket is activated and a non-mutating measurement can be run without adding harness code.
- Comparison notes that state known benchmark bias and omitted system features.

## Explicit exclusions

No benchmark harness implementation, no generated benchmark fixtures committed to the repo, no CI integration, no optimization, no public performance claim, no dependency addition, and no modification to existing quality gates.

## References

- `.10x/tickets/2026-07-07-performance-investigation-backlog.md`
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `crates/cdf-engine/**`
- `crates/cdf-package/**`
- `crates/cdf-formats/**`
- `crates/cdf-dest-*/*`

## Progress and notes

- 2026-07-07: Opened from qualitative performance discussion. Current claim to validate: CDF should be fast for governed, typed, batch-oriented local data movement, but should not claim to beat DuckDB/DataFusion/Polars in their native hot paths without evidence.
- 2026-07-07: Activated for triage after the run spine parent closed. Scope remains research/record-only: inspect code paths, classify benchmark candidates, and open a bounded harness ticket if recommended; do not implement benches in this ticket.
- 2026-07-07: Cancelled before closure because the user-ratified P0 structural-debt directive promoted the benchmark gate directly to implementation. The replacement implementation owner is `.10x/tickets/done/2026-07-07-p0-workstream-f-benchmark-gate.md`.

## Blockers

Superseded by `.10x/tickets/done/2026-07-07-p0-workstream-f-benchmark-gate.md`.
