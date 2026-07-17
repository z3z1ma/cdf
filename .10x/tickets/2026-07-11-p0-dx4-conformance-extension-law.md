Status: open
Created: 2026-07-11
Updated: 2026-07-16
Parent: .10x/tickets/2026-07-11-p0-destination-extension-boundary.md
Depends-On: .10x/tickets/2026-07-11-p0-dx3-generic-lock-doctor-replay.md

# P0 DX4: destination extension conformance and build-graph law

## Scope

Replace repeated concrete conformance enums/factories with one data-driven adapter catalog, add the fourth-driver extension law and static dependency/import gates, and measure the Cargo rebuild graph for a destination-only edit.

## Acceptance criteria

- One fixture catalog entry enrolls a destination in every applicable shared law.
- Generic conformance assertions contain no destination-name match arms.
- Static tests prevent `cdf-project`/generic CLI imports of concrete destinations.
- Before/after `cargo build --timings` or equivalent evidence shows a destination-only edit no longer rebuilds destination-neutral runtime/project crates unnecessarily.

## Blockers

Depends on DX3.

## Progress and notes

- 2026-07-11: Strict all-target inspection confirms the concrete destination conformance factories are stale after destination constructors moved behind runtime adapters: `cdf-conformance` still calls removed `ResolvedProjectDestination::{duckdb,postgres,parquet_filesystem}` constructors, and one run-matrix path calls removed `RestResource::compiled`. This directly validates DX4's catalog migration scope; production/source checks remain independent.
- 2026-07-11: Added the single conformance destination catalog and moved live-run, run-matrix, runtime-chaos, drift-quarantine, and acceptance-demo destination resolution through the same `DestinationRegistry`/driver authority as production. The removed project constructors remain deleted; no compatibility shim was restored. Static tests pin the three first-party registrations and prohibit concrete destination imports outside the explicit CLI composition/adapter diagnostics and temporary test-only project helper owner. Strict all-target Clippy and the four 100-rebuild package goldens pass. The live DuckDB golden now reaches execution and fails at the independent source-extension boundary because its fixture has not yet resolved the neutral source plan; `.10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md` owns that migration. Evidence: `.10x/evidence/2026-07-11-p0-dx4-conformance-catalog-milestone.md`.
- 2026-07-11: Corrected the conformance crate boundary after a downstream DuckDB test build proved that public live-run helpers cannot call `cfg(test)`-only catalogs. Destination/source fixture catalogs and their adapter dependencies are now ordinary conformance-harness library code; downstream destination suites compile the exact same public helper implementation. The 24-test DuckDB suite passes and strict all-target DuckDB Clippy plus normal `cdf-conformance` check are green.
- 2026-07-16: SX1 closure routed the remaining external-source law breadth here. DX4's data-driven catalog must cover retry/identity change, cancellation, memory failure, redaction/egress, projection/filter/limit, deep validation, and jobs invariance without reopening the source extension implementation ticket.
