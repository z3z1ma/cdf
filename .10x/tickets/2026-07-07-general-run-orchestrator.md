Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-run-spine-implementation-program.md
Depends-On: .10x/tickets/done/2026-07-07-duckdb-commit-session-refactor.md, .10x/tickets/done/2026-07-07-parquet-commit-session-refactor.md, .10x/tickets/done/2026-07-07-postgres-commit-session-refactor.md, .10x/tickets/2026-07-07-run-ledger-store.md, .10x/specs/run-orchestration-ledger.md

# Implement the general project run orchestrator

## Scope

Replace the hard-coded local-file-to-DuckDB orchestration path with a general project runtime over `ResourceStream`, package building, destination commit sessions, checkpoint stores, and the run ledger.

Owns:

- `crates/cdf-project/src/runtime.rs` and supporting modules.
- Compatibility wrappers for current local DuckDB/file conformance callers.
- Focused integration tests for file, REST, and table-backed Postgres SQL resources where lower-layer runtime dependencies are deterministic.

## Acceptance criteria

- One orchestrator can run supported resource streams into supported destinations through the commit-session API.
- The orchestrator records run ledger events in the order specified by `.10x/specs/run-orchestration-ledger.md`.
- Existing local-file-to-DuckDB tests and golden conformance pass through compatibility wrappers with no unnecessary fixture churn.
- Crash-window recovery uses package/receipt/checkpoint evidence and does not contact sources after package finalization.
- Unsupported combinations fail closed before source, package, destination, or checkpoint mutation.

## Evidence expectations

Run focused `cdf-project` tests, conformance live-run/chaos tests, relevant destination/source tests, clippy, nextest over touched crates, and review mapping each crash window to evidence.

## Explicit exclusions

No CLI command parsing, no `inspect run` presentation, no distributed scheduler, no resident streaming, no arbitrary SQL query execution, no live external HTTP credentials, no warehouse destinations, no performance optimization.

## Design notes

- 2026-07-07: Destination commit-session refactors proved the current portable kernel `DestinationProtocol::begin(request, plan)` inputs are not enough by themselves to commit a package. DuckDB and Parquet require package path/schema/package replay context captured by package-aware planning, and Postgres currently uses `with_commit_request` as a compatibility handoff. This ticket must either route generic runs through package-aware destination planning before `begin`, or explicitly supersede/refine the kernel session input contract so package replay inputs are first-class.

## Blockers

Blocked until the run-ledger store is done.
