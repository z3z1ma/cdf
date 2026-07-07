Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/2026-07-07-p0-structural-debt-program.md, .10x/decisions/p0-structural-debt-stop-line.md

# P0 structural debt activation evidence

## What was observed

The user ratified a P0 stop-the-line directive requiring six structural-debt workstreams before new destination lanes, new source archetype lanes, or the resident streaming supervisor can proceed.

Current source and records support the directive:

- `crates/cdf-kernel/src/destination.rs` currently defines `CommitSession::write(&mut self) -> Result<()>` with no segment payload and gives `DestinationProtocol::begin` an error-returning default.
- `crates/cdf-project/src/runtime.rs` is 2,913 lines and contains closed `ProjectRunDestination` and `ProjectRunResource` enums plus public destination-specialized replay/recover wrapper families.
- Existing conformance records show many closed slices but still leave non-DuckDB chaos, broader run-spine matrix coverage, per-destination live-run goldens, property/fuzz targets, and the killer-demo evidence open under `.10x/tickets/2026-07-05-conformance-chaos-golden.md`.
- `.10x/tickets/done/2026-07-07-duckdb-arrow58-transitive-residual.md` already owns the remaining DuckDB Arrow 58 investigation after the DataFusion git pin aligned the engine tuple.
- Current contract implementation has schema/program vocabulary and package quarantine artifact helpers, but live row-level verdict routing is not complete.
- No benchmark harness is present; the prior baseline benchmark owner was triage-only.

## Procedure

Inspected:

- `VISION.md` sections for DataFusion, runtime, contracts, destinations, conformance, MVP, and dependency policy.
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/run-orchestration-ledger.md`
- `.10x/specs/types-contracts-normalization.md`
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/knowledge/vision-coverage-matrix.md`
- `.10x/knowledge/quality-gate-execution.md`
- active destination/source/conformance/performance tickets.
- `crates/cdf-kernel/src/destination.rs`
- `crates/cdf-project/src/runtime.rs`
- crate/module inventories for `cdf-conformance`, `cdf-contract`, destination crates, and package reader/builder paths.

Created or updated:

- `.10x/decisions/p0-structural-debt-stop-line.md`
- `.10x/tickets/2026-07-07-p0-structural-debt-program.md`
- six P0 workstream child tickets A-F.
- `.10x/knowledge/runtime-conformance-throughput-rule.md`
- destination and run specs to require segment-streaming sessions and trait-level receipt verification.
- `cdf` system, conformance, performance, lakehouse/warehouse, CDC/streaming, and coverage-matrix records.
- cancelled `.10x/tickets/cancelled/2026-07-07-performance-baseline-benchmark-suite.md` in favor of P0 Workstream F.

Read-only subagent inventory independently confirmed the source/record gaps and reported no edits or test/build runs.

Tool availability check found `jscpd`, `rust-code-analysis-cli`, and `scc` installed locally for the P0 child tickets that require duplication, complexity, and raw-size evidence.

## What this supports

The `.10x/` graph now has a P0 parent, six child owners, stop-line blockers on affected parent tickets, updated specs where the directive superseded optional session language, and a coverage-matrix row tracking the program.

The benchmark gate has a single active owner: `.10x/tickets/2026-07-07-p0-workstream-f-benchmark-gate.md`.

## Limits

This evidence proves record activation and source/record inspection only. It does not prove implementation of any P0 workstream. No Rust build, test, jscpd, rust-code-analysis, benchmark, or supply-chain gate was required for this record-only activation.
