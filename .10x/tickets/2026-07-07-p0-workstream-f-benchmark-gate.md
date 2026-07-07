Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-p0-structural-debt-program.md

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

## Blockers

None for the baseline harness. Streaming-commit performance work depends on Workstream A.
