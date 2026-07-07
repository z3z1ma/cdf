Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/done/2026-07-07-run-spine-implementation-program.md
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

## Progress and notes

- 2026-07-07: Worker implemented Postgres commit-session support. `PostgresDestination::begin` now returns a phase-checked Postgres session when the destination is prepared with the existing `PostgresCommitRequest`; legacy `commit_package` is preserved as a wrapper over the same session path. The session keeps system DDL, duplicate lookup, target DDL/load, mirrors, receipt verification, and commit/abort inside one explicit Postgres transaction, and still records package receipts best-effort after durable commit.
- 2026-07-07: Added live Postgres session coverage for `DestinationProtocol::begin` returning a verifiable receipt, duplicate replay remaining a no-op with one package receipt, and abort rolling back system migrations. Existing live append, replace, merge, rollback, duplicate, mirror, decimal, and receipt verification tests still run beside the session path.
- 2026-07-07: Verification passed: `cargo fmt --all -- --check`; `cargo check -p cdf-dest-postgres --locked`; `cargo test -p cdf-dest-postgres --locked --no-fail-fast` passed with 27 tests including live local Postgres; `cargo clippy -p cdf-dest-postgres --all-targets --locked -- -D warnings`; `cargo nextest run -p cdf-dest-postgres --locked` passed with 27 tests; `cargo semver-checks -p cdf-dest-postgres --baseline-rev HEAD` passed; `git diff --check -- crates/cdf-dest-postgres/src/api.rs crates/cdf-dest-postgres/src/commit.rs crates/cdf-dest-postgres/src/lib.rs crates/cdf-dest-postgres/src/live_tests.rs crates/cdf-dest-postgres/src/sheet.rs` passed.
- 2026-07-07: Parent verification passed for the combined destination refactor slice, including workspace tests and source/security checks. Evidence: `.10x/evidence/2026-07-07-destination-commit-session-refactors.md`. Review: `.10x/reviews/2026-07-07-destination-commit-session-refactors-review.md`.
