Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-07-run-ledger-commit-session-spine-ratification.md

# Implement the general run spine

## Scope

Parent plan for P0 run-spine implementation after ratifying `.10x/decisions/run-ledger-commit-session-spine.md` and `.10x/specs/run-orchestration-ledger.md`.

Children:

- `.10x/tickets/done/2026-07-07-kernel-destination-commit-session-api.md`
- `.10x/tickets/done/2026-07-07-duckdb-commit-session-refactor.md`
- `.10x/tickets/done/2026-07-07-parquet-commit-session-refactor.md`
- `.10x/tickets/done/2026-07-07-postgres-commit-session-refactor.md`
- `.10x/tickets/done/2026-07-07-run-ledger-store.md`
- `.10x/tickets/done/2026-07-07-general-run-orchestrator.md`
- `.10x/tickets/done/2026-07-07-non-file-window-close-checkpoint-semantics.md`
- `.10x/tickets/done/2026-07-07-cli-run-resume-replay-inspect-spine.md`

## Acceptance criteria

- Kernel, destinations, project runtime, run ledger, and CLI all compose through one run spine.
- Existing specialized DuckDB/file runtime behavior remains available as thin compatibility wrappers until callers migrate.
- No checkpoint head advances outside `CheckpointStore::commit`.
- The MVP demonstration path can be implemented as a run-spine consumer rather than another specialized runtime.

## Evidence

- `.10x/evidence/2026-07-07-run-spine-implementation-program.md`
- Child evidence records linked from the child tickets listed above.

## Review

- `.10x/reviews/2026-07-07-run-spine-implementation-program-review.md`

## Progress and notes

- 2026-07-07: Opened after ratification of the run-ledger and commit-session decision/spec. Parent is not executable; assign child tickets to workers.
- 2026-07-07: Kernel commit-session API child closed with evidence `.10x/evidence/2026-07-07-kernel-destination-commit-session-api.md` and review `.10x/reviews/2026-07-07-kernel-destination-commit-session-api-review.md`. Destination refactor children are now unblocked on the kernel API dependency.
- 2026-07-07: DuckDB, Parquet, and Postgres destination commit-session refactor children closed with combined evidence `.10x/evidence/2026-07-07-destination-commit-session-refactors.md` and review `.10x/reviews/2026-07-07-destination-commit-session-refactors-review.md`. The general orchestrator is now blocked on the run-ledger store and must address package-aware session context handoff.
- 2026-07-07: SQLite run-ledger store child closed with evidence `.10x/evidence/2026-07-07-sqlite-run-ledger-store.md` and review `.10x/reviews/2026-07-07-sqlite-run-ledger-store-review.md`. The general orchestrator child is now unblocked.
- 2026-07-07: General run orchestrator child has a verified partial implementation for deterministic local file resources into DuckDB and filesystem Parquet, with evidence `.10x/evidence/2026-07-07-general-run-orchestrator-partial.md` and review `.10x/reviews/2026-07-07-general-run-orchestrator-partial-review.md`. The child is blocked, not closed, on Postgres destination request semantics and non-file resource stream semantics.
- 2026-07-07: User ratified the remaining Postgres destination and non-file checkpoint semantics. General orchestrator and CLI spine work are open dependency-gated children; no parent-level semantic blocker remains.
- 2026-07-07: General run Postgres destination child closed with evidence `.10x/evidence/2026-07-07-general-run-postgres-destination.md` and review `.10x/reviews/2026-07-07-general-run-postgres-destination-review.md`. The general orchestrator child now needs a parent closure audit; CLI spine remains an open executable child for the next implementation goal.
- 2026-07-07: General run orchestrator child closed with evidence `.10x/evidence/2026-07-07-general-run-orchestrator-closure.md` and review `.10x/reviews/2026-07-07-general-run-orchestrator-closure-review.md`. At that point, CLI run/resume/replay/inspect was the remaining open run-spine implementation child.
- 2026-07-07: Split CLI spine into child tickets for `run`, `replay package`, `resume`, and `inspect run` so execution can proceed without one monolithic CLI patch.
- 2026-07-07: Non-file window-close checkpoint semantics child closed with evidence `.10x/evidence/2026-07-07-non-file-window-close-checkpoint-semantics.md` and review `.10x/reviews/2026-07-07-non-file-window-close-checkpoint-semantics.md`. Project-run checkpoint advancement now supports ratified numeric/timestamp/date cursor window-close semantics and keeps unratified page-token/mixed/unsupported variants fail-closed.
- 2026-07-07: CLI filesystem Parquet `run` and `replay package` slices landed through the general run/replay spine with evidence `.10x/evidence/2026-07-07-cli-parquet-run-replay.md`. The CLI spine parent remained open for REST/Postgres run wiring, Postgres replay, resume, and inspect-run.
- 2026-07-07: CLI `replay package` child closed at `.10x/tickets/done/2026-07-07-cli-replay-package-spine.md` after Postgres artifact replay wiring landed. At that point, the CLI spine parent still needed REST/Postgres `run`, `resume`, and `inspect run`.
- 2026-07-07: CLI `inspect run` child closed at `.10x/tickets/done/2026-07-07-cli-inspect-run-spine.md` with read-only run-ledger reporting evidence. At that point, the CLI spine parent still needed `resume`.
- 2026-07-07: CLI `resume` child closed at `.10x/tickets/done/2026-07-07-cli-resume-spine.md`, including DuckDB, filesystem Parquet, and Postgres finalized-package/no-receipt replay without source contact. CLI spine parent remained open after closure audit for the SQL success child now closed at `.10x/tickets/done/2026-07-07-cli-sql-run-success.md`.
- 2026-07-07: CLI SQL run success child closed at `.10x/tickets/done/2026-07-07-cli-sql-run-success.md`, adding direct live CLI evidence for `cdf run` over a table-backed Postgres SQL resource with an ordered cursor.
- 2026-07-07: CLI aggregate parent closed at `.10x/tickets/done/2026-07-07-cli-run-resume-replay-inspect-spine.md`; all run-spine implementation children are now done. Closed this parent with aggregate evidence `.10x/evidence/2026-07-07-run-spine-implementation-program.md` and review `.10x/reviews/2026-07-07-run-spine-implementation-program-review.md`.

## Blockers

None.
