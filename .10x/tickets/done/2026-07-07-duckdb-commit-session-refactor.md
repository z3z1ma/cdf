Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/done/2026-07-07-run-spine-implementation-program.md
Depends-On: .10x/tickets/done/2026-07-07-kernel-destination-commit-session-api.md, .10x/specs/destination-receipts-guarantees.md

# Refactor DuckDB onto commit sessions

## Scope

Make the DuckDB destination consume the kernel commit-session API while preserving existing package commit behavior, receipts, duplicate handling, mirrors, and single-writer constraints.

Owns:

- `crates/cdf-dest-duckdb/**`
- Destination conformance tests touched only for DuckDB session coverage.

## Acceptance criteria

- DuckDB implements the commit-session API.
- Existing DuckDB commit entry points remain as wrappers or compatibility facades.
- Append, replace, and merge receipt semantics remain unchanged.
- Duplicate package-token behavior still returns no-op receipts where currently supported.
- Existing local DuckDB lifecycle chaos and golden conformance callers still pass.

## Evidence expectations

Run DuckDB destination tests, destination conformance for DuckDB, relevant `cdf-project` local DuckDB lifecycle tests, clippy for touched crates, and semver checks where public API changes.

## Explicit exclusions

No Parquet or Postgres destination edits, no general project orchestrator, no CLI wiring, no run ledger store, no performance optimization.

## Blockers

Unblocked by `.10x/tickets/done/2026-07-07-kernel-destination-commit-session-api.md`.

## Progress and notes

- 2026-07-07: Implemented DuckDB commit sessions by wrapping the existing `commit_package` mutation path in `DuckDbCommitSession`; `DestinationProtocol::begin` now returns a session after `plan_package_commit` registers the DuckDB package-specific context required by the current kernel API. Existing public `commit_package` remains as a compatibility facade through the session and preserves the old writer lock, mirror, receipt, and package receipt path.
- 2026-07-07: Added focused DuckDB tests for begin/session receipt shape and duplicate replay no-op behavior. Verified: `cargo fmt --all -- --check`; `cargo fmt --package cdf-dest-duckdb -- --check`; `cargo check -p cdf-dest-duckdb --lib --locked`; `cargo test -p cdf-dest-duckdb --locked --no-fail-fast`; `cargo clippy -p cdf-dest-duckdb --all-targets --locked -- -D warnings`; `cargo clippy -p cdf-dest-duckdb --lib --locked -- -D warnings`; `cargo test -p cdf-project --locked --no-fail-fast`; `cargo semver-checks -p cdf-dest-duckdb --baseline-rev HEAD`; `git diff --check -- crates/cdf-dest-duckdb/src/api.rs crates/cdf-dest-duckdb/src/lib.rs crates/cdf-dest-duckdb/src/tests.rs`.
- 2026-07-07: An initial `cargo test -p cdf-dest-duckdb --locked --no-fail-fast` attempt was temporarily blocked by unrelated in-progress Postgres compile errors; rerunning after the concurrent edits settled passed.
- 2026-07-07: Parent verification passed for the combined destination refactor slice. Evidence: `.10x/evidence/2026-07-07-destination-commit-session-refactors.md`. Review: `.10x/reviews/2026-07-07-destination-commit-session-refactors-review.md`.
