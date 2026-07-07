Status: open
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
