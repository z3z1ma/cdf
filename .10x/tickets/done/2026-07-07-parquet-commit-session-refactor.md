Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-run-spine-implementation-program.md
Depends-On: .10x/tickets/done/2026-07-07-kernel-destination-commit-session-api.md, .10x/specs/destination-receipts-guarantees.md

# Refactor Parquet destination onto commit sessions

## Scope

Make the Parquet/object-store destination consume the kernel commit-session API while preserving manifest receipt behavior, idempotent object writes, and archive-path semantics.

Owns:

- `crates/cdf-dest-parquet/**`
- Destination conformance tests touched only for Parquet session coverage.

## Acceptance criteria

- Parquet/object-store implements the commit-session API.
- Existing Parquet commit entry points remain as wrappers or compatibility facades.
- Manifest receipts still include verifiable object keys, etags or hashes where supported, row counts, and schema/package identity.
- Duplicate replay behavior remains unchanged.
- The Iceberg/Delta seam remains intact: no lakehouse semantics are introduced here.

## Evidence expectations

Run Parquet destination tests, destination conformance for Parquet, package/archive-adjacent tests if touched, clippy for touched crates, and semver checks where public API changes.

## Explicit exclusions

No DuckDB or Postgres destination edits, no general project orchestrator, no CLI wiring, no run ledger store, no Iceberg/Delta implementation, no performance optimization.

## Blockers

Unblocked by `.10x/tickets/done/2026-07-07-kernel-destination-commit-session-api.md`.

## Progress and notes

- 2026-07-07: Implemented `ParquetDestination` commit-session support for `DestinationProtocol::begin` by consuming package context captured by `plan_package_commit`. The session models apply-migrations/write/finalize/abort and writes through the existing `commit_package` behavior to preserve manifest receipts, duplicate replay, package receipt recording, replace-pointer semantics, and the Iceberg/Delta seam.
- 2026-07-07: Added focused Parquet tests for first-write session receipt verification, duplicate replay through the session, and abort-before-write leaving no manifest or package receipt.
- 2026-07-07: Owned-surface verification passed: `cargo check -p cdf-dest-parquet --lib --locked`, `cargo clippy -p cdf-dest-parquet --lib --locked -- -D warnings`, `cargo fmt -p cdf-dest-parquet -- --check`, and `git diff --check -- crates/cdf-dest-parquet/src/api.rs crates/cdf-dest-parquet/src/lib.rs crates/cdf-dest-parquet/src/tests.rs`. Full `cargo fmt --all -- --check`, `cargo test -p cdf-dest-parquet --locked --no-fail-fast`, and `cargo clippy -p cdf-dest-parquet --all-targets --locked -- -D warnings` were blocked by concurrent unrelated DuckDB/Postgres worktree edits outside this child ticket scope.
- 2026-07-07: Parent verification passed for full Parquet tests and combined destination quality checks after concurrent destination edits settled. Evidence: `.10x/evidence/2026-07-07-destination-commit-session-refactors.md`. Review: `.10x/reviews/2026-07-07-destination-commit-session-refactors-review.md`.
