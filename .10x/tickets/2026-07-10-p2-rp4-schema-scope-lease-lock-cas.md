Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/specs/schema-promotion-corrections.md, .10x/specs/checkpoint-state-commit-gate.md

# P2 RP4 fenced schema-scope lease and atomic lock compare-and-swap

## Scope

Add an executor-neutral fenced lease primitive over `ScopeKey` with in-memory and SQLite implementations, then add crash-safe atomic `cdf.lock` compare-and-swap against exact prior bytes/hash. This supplies promotion concurrency safety and a reusable seam for later distributed stores without implementing a scheduler.

## Acceptance criteria

- Kernel lease values include scope, owner, fencing token, acquired/expiry times, and explicit renew/release semantics.
- Only the current unexpired fencing token may perform guarded promotion publication; stale owners fail closed.
- In-memory/SQLite stores pass shared acquire/contention/expiry/renew/release/fencing conformance.
- Lease persistence/migrations preserve current checkpoint store compatibility and commit-gate APIs.
- Atomic lock CAS writes a temporary file, syncs where supported, rename-over installs, and refuses changed prior bytes/hash.
- Failpoints cover crash before temp sync, before rename, after rename, and stale-token publication.
- Models contain no CLI, local executor, Spark/Flink, or destination-driver dependencies.

## Evidence expectations

Store conformance, concurrent contention tests, failpoint/crash tests, filesystem atomicity evidence/limits, migration fixtures, and coordination review with the distributed execution ticket.

## Explicit exclusions

No worker scheduler, remote lease store, destination correction, promotion planner, or package execution.

## Progress and notes

- 2026-07-10: Opened from the ratified schema-lease requirement; general distributed scheduling remains separately owned.

## Blockers

None.
