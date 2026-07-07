Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-run-spine-implementation-program.md
Depends-On: .10x/tickets/done/2026-07-07-kernel-destination-commit-session-api.md, .10x/specs/destination-receipts-guarantees.md

# Refactor Postgres destination onto commit sessions

## Scope

Make the Postgres destination consume the kernel commit-session API while preserving transactional DDL/load behavior, xid-bearing receipts, mirrors, rollback behavior, duplicate handling, and decimal/type fidelity.

Owns:

- `crates/cdf-dest-postgres/**`
- Destination conformance tests touched only for Postgres session coverage.

## Acceptance criteria

- Postgres implements the commit-session API.
- Existing Postgres commit entry points remain as wrappers or compatibility facades.
- Live local Postgres tests prove append, replace, merge, rollback, duplicate, mirror, and receipt verification behavior still works through or beside the session path.
- Source-side Postgres SQL runtime from `.10x/tickets/done/2026-07-07-declarative-postgres-sql-resource-execution.md` remains unaffected.

## Evidence expectations

Run Postgres destination tests including live local coverage, destination conformance for Postgres, clippy for touched crates, nextest where practical, and semver checks where public API changes.

## Explicit exclusions

No DuckDB or Parquet destination edits, no Postgres source runtime changes unless required for compile, no general project orchestrator, no CLI wiring, no run ledger store, no connection pool.

## Blockers

Unblocked by `.10x/tickets/done/2026-07-07-kernel-destination-commit-session-api.md`.
