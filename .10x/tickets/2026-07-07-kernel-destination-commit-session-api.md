Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-run-spine-implementation-program.md
Depends-On: .10x/decisions/run-ledger-commit-session-spine.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/destination-receipts-guarantees.md

# Add kernel destination commit-session API

## Scope

Add the driver-neutral commit-session API to `cdf-kernel` without refactoring concrete destinations yet.

Owns:

- `crates/cdf-kernel/src/destination.rs`
- Kernel tests or conformance fixtures needed to express the API contract.

## Acceptance criteria

- `DestinationProtocol` exposes a `begin` operation or equivalent additive API returning a commit-session trait object.
- The commit-session trait models migration/write/finalize/abort phases and makes `finalize` return a durable `Receipt` or an error.
- The API preserves existing `sheet` and `plan_commit` behavior.
- Public API compatibility impact is measured with `cargo semver-checks`; any intentional break is recorded before closure.
- No concrete destination behavior changes in this child.

## Evidence expectations

Run focused kernel checks, workspace check, clippy for `cdf-kernel`, semver checks for `cdf-kernel`, and a review proving the API cannot bypass receipt verification.

## Explicit exclusions

No DuckDB/Parquet/Postgres refactors, no project runtime changes, no CLI changes, no run ledger schema, no package fixture updates.

## Blockers

None.
