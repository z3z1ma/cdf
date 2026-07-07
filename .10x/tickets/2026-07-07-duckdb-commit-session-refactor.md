Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-run-spine-implementation-program.md
Depends-On: .10x/tickets/2026-07-07-kernel-destination-commit-session-api.md, .10x/specs/destination-receipts-guarantees.md

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

Blocked until `.10x/tickets/2026-07-07-kernel-destination-commit-session-api.md` is done.
