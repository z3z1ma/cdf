Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-run-spine-implementation-program.md
Depends-On: .10x/decisions/run-ledger-commit-session-spine.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# Implement the SQLite run ledger store

## Scope

Add the first project-owned append-only run ledger store for local environments.

Owns the lowest coherent storage/API slice, likely across:

- `crates/cdf-kernel/src/ids.rs` only if additional typed ids are needed.
- `crates/cdf-project/**` or `crates/cdf-state-sqlite/**` depending on where the run store trait and SQLite implementation fit best after inspection.
- Tests for event ordering, collision behavior, redaction-safe serialization, and recovery queries.

## Acceptance criteria

- The runtime can mint or accept a `RunId` and fail closed on caller-supplied id collision.
- Run events are append-only with monotonic per-run sequence numbers.
- Required event families from `.10x/specs/run-orchestration-ledger.md` can be serialized with secret references only.
- The run ledger cannot write committed checkpoints or bypass `CheckpointStore::commit`.
- Query APIs support `inspect run` and `resume` callers without requiring source contact.
- Schema versioning/migration story is recorded if new SQLite tables are introduced.

## Evidence expectations

Run focused project/state tests, workspace check, clippy for touched crates, direct redaction tests, and `git diff --check`.

## Explicit exclusions

No general orchestrator, no destination sessions, no CLI command wiring beyond test helpers, no non-SQLite backend, no distributed leases, no package format changes unless a run association artifact is explicitly scoped.

## Blockers

None.
