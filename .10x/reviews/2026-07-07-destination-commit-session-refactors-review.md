Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: Destination commit-session refactors for DuckDB, Parquet, and Postgres
Verdict: pass

# Destination commit-session refactors review

## Target

Review of the destination refactor slice owned by:

- `.10x/tickets/done/2026-07-07-duckdb-commit-session-refactor.md`
- `.10x/tickets/done/2026-07-07-parquet-commit-session-refactor.md`
- `.10x/tickets/done/2026-07-07-postgres-commit-session-refactor.md`

Implementation surfaces reviewed:

- `crates/cdf-dest-duckdb/src/api.rs`
- `crates/cdf-dest-duckdb/src/lib.rs`
- `crates/cdf-dest-duckdb/src/tests.rs`
- `crates/cdf-dest-parquet/src/api.rs`
- `crates/cdf-dest-parquet/src/lib.rs`
- `crates/cdf-dest-parquet/src/tests.rs`
- `crates/cdf-dest-postgres/src/api.rs`
- `crates/cdf-dest-postgres/src/commit.rs`
- `crates/cdf-dest-postgres/src/lib.rs`
- `crates/cdf-dest-postgres/src/live_tests.rs`
- `crates/cdf-dest-postgres/src/sheet.rs`

## Findings

No blocking correctness findings were found.

Significant residual design constraint: the kernel `DestinationProtocol::begin(request, plan)` surface does not carry package path, package schema, merge-key, package view, or destination-specific replay context. DuckDB and Parquet bridge this by remembering package context from `plan_package_commit`; Postgres bridges it with `with_commit_request`. This is acceptable for the current refactor because the tickets required compatibility with existing package commit paths, but the general orchestrator must make package-aware session input first-class or deliberately refine the kernel API. This is recorded on `.10x/tickets/2026-07-07-general-run-orchestrator.md`.

Minor semantic limit: DuckDB and Parquet session `write` remains the prior durable destination commit boundary, so `finalize` returns an already durable receipt rather than performing a separate destination commit. Postgres does perform the transaction commit during `finalize`. This preserves current behavior and avoids introducing unratified streaming/restartable write semantics.

Minor operational limit: Postgres session rollback is explicit through `abort`, and failed writes also rely on Postgres connection teardown to roll back any open transaction if the client has been taken during an error path. This is consistent with Postgres transaction behavior and covered by existing rollback/live failure tests, but finer-grained failed-write rollback observability is not part of this ticket.

## Assumptions Tested

The review checked that existing public commit entry points remain available, session tests exercise receipt verification and duplicate behavior, Postgres session tests cover live local transaction finalization and abort rollback, and quality evidence includes workspace tests plus focused destination checks.

The review also checked that the residual package-context handoff risk has a durable owner instead of being hidden in implementation comments only.

## Verdict

Pass. The three destination tickets satisfy their current acceptance criteria and preserve receipt/idempotency behavior. The remaining package-aware session input design belongs to the general orchestrator ticket before generic `run` wiring.

## Residual Risk

The next orchestrator slice must not treat the portable `DestinationCommitRequest` plus `CommitPlan` as sufficient to commit a package. It must either route through package-aware planning/context capture or supersede/refine the kernel session input contract with explicit package replay inputs.
