Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-07-run-ledger-commit-session-spine-ratification.md

# Implement the general run spine

## Scope

Parent plan for P0 run-spine implementation after ratifying `.10x/decisions/run-ledger-commit-session-spine.md` and `.10x/specs/run-orchestration-ledger.md`.

Children:

- `.10x/tickets/done/2026-07-07-kernel-destination-commit-session-api.md`
- `.10x/tickets/2026-07-07-duckdb-commit-session-refactor.md`
- `.10x/tickets/2026-07-07-parquet-commit-session-refactor.md`
- `.10x/tickets/2026-07-07-postgres-commit-session-refactor.md`
- `.10x/tickets/2026-07-07-run-ledger-store.md`
- `.10x/tickets/2026-07-07-general-run-orchestrator.md`
- `.10x/tickets/2026-07-07-cli-run-resume-replay-inspect-spine.md`

## Acceptance criteria

- Kernel, destinations, project runtime, run ledger, and CLI all compose through one run spine.
- Existing specialized DuckDB/file runtime behavior remains available as thin compatibility wrappers until callers migrate.
- No checkpoint head advances outside `CheckpointStore::commit`.
- The MVP demonstration path can be implemented as a run-spine consumer rather than another specialized runtime.

## Progress and notes

- 2026-07-07: Opened after ratification of the run-ledger and commit-session decision/spec. Parent is not executable; assign child tickets to workers.
- 2026-07-07: Kernel commit-session API child closed with evidence `.10x/evidence/2026-07-07-kernel-destination-commit-session-api.md` and review `.10x/reviews/2026-07-07-kernel-destination-commit-session-api-review.md`. Destination refactor children are now unblocked on the kernel API dependency.

## Blockers

None at parent level. Child dependencies sequence the implementation.
