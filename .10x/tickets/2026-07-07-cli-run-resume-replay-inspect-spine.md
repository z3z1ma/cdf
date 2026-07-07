Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-run-spine-implementation-program.md
Depends-On: .10x/tickets/done/2026-07-07-general-run-orchestrator.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# Wire CLI run, resume, replay, and inspect-run to the spine

## Scope

Parent plan for wiring the existing CLI command surface to the general run spine now that the lower-layer orchestrator exists.

Children:

- `.10x/tickets/done/2026-07-07-cli-run-general-runtime.md`
- `.10x/tickets/done/2026-07-07-cli-replay-package-spine.md`
- `.10x/tickets/done/2026-07-07-cli-resume-spine.md`
- `.10x/tickets/done/2026-07-07-cli-inspect-run-spine.md`
- `.10x/tickets/2026-07-07-cli-sql-run-success.md`

Owns:

- `crates/cdf-cli/**`
- CLI-facing project/runtime adapters needed only for command plumbing.
- JSON/human output tests for run, resume, replay package, and inspect run.

## Acceptance criteria

- `cdf run` no longer rejects supported REST, SQL table, file, DuckDB, Parquet, or Postgres combinations solely because the runtime was specialized.
- `cdf resume` drains interrupted work through the run ledger and crash matrix without source contact after package finalization.
- `cdf replay package <pkg> --to <dest>` creates a run, records duplicate receipts, and preserves package replay determinism.
- `cdf inspect run <id>` shows plan, verdict summaries, receipts, transitions, package/checkpoint pointers, duplicate status, and recovery guidance without leaking secrets.
- `--json` output is stable for automation-relevant fields, and human output remains scheduler-friendly.

## Evidence expectations

Run focused CLI tests, JSON snapshot/assertion tests, redaction tests, no-write failure tests, relevant project runtime tests, clippy for CLI/project, and `git diff --check`.

## Explicit exclusions

No lower-layer invariant implementation, no UI, no daemon, no distributed scheduler, no streaming supervisor, no new source/destination semantics, no package archive work.

## Blockers

None at parent level. Execute the child tickets above; do not implement this parent directly.

## Progress and notes

- 2026-07-07: User ratified the outstanding run-spine adjacent decisions. This ticket is open and dependency-gated by the general orchestrator, not waiting on semantic input.
- 2026-07-07: General orchestrator dependency closed in `.10x/tickets/done/2026-07-07-general-run-orchestrator.md`; this CLI spine ticket is now the remaining run-spine executable child.
- 2026-07-07: Split this broad ticket into executable children for `run`, `replay package`, `resume`, and `inspect run`. The first implementable child was `.10x/tickets/done/2026-07-07-cli-run-general-runtime.md`; `resume` is intentionally sequenced after run/replay destination parsing.
- 2026-07-07: The filesystem Parquet portions of `cdf run` and `cdf replay package` landed under child tickets `.10x/tickets/done/2026-07-07-cli-run-general-runtime.md` and `.10x/tickets/done/2026-07-07-cli-replay-package-spine.md`, with shared evidence `.10x/evidence/2026-07-07-cli-parquet-run-replay.md` and review `.10x/reviews/2026-07-07-cli-parquet-run-replay-review.md`. This parent remained open for REST/Postgres run wiring, Postgres replay, resume, and inspect-run.
- 2026-07-07: `cdf replay package` child closed at `.10x/tickets/done/2026-07-07-cli-replay-package-spine.md` after Postgres replay wiring landed with evidence `.10x/evidence/2026-07-07-cli-postgres-package-replay.md` and review `.10x/reviews/2026-07-07-cli-postgres-package-replay-review.md`. This parent remains open for REST/Postgres `run`, `resume`, and `inspect run`.
- 2026-07-07: `cdf run` child closed at `.10x/tickets/done/2026-07-07-cli-run-general-runtime.md` after REST and Postgres destination wiring landed with evidence `.10x/evidence/2026-07-07-cli-rest-postgres-run-quality.md` and review `.10x/reviews/2026-07-07-cli-rest-postgres-run-review.md`. This parent remains open for `resume` and `inspect run`.
- 2026-07-07: `cdf inspect run` child closed at `.10x/tickets/done/2026-07-07-cli-inspect-run-spine.md` after read-only run-ledger inspection, redacted JSON/human output, artifact status reporting, and quality metrics landed with evidence `.10x/evidence/2026-07-07-cli-inspect-run-spine.md` and review `.10x/reviews/2026-07-07-cli-inspect-run-spine-review.md`. This parent remains open for `resume`.
- 2026-07-07: `cdf resume` child closed at `.10x/tickets/done/2026-07-07-cli-resume-spine.md` after run-id-scoped recovery landed for terminal no-op, fail-closed missing/inconsistent evidence, DuckDB/Parquet/Postgres finalized-package replay without source contact, durable-receipt recovery, and exact-head stale-status repair. Evidence: `.10x/evidence/2026-07-07-cli-resume-spine.md`; review: `.10x/reviews/2026-07-07-cli-resume-spine-review.md`.
- 2026-07-07: Parent closure audit recorded concerns in `.10x/reviews/2026-07-07-cli-spine-parent-closure-audit.md`: lower `cdf-project` SQL execution exists, but direct CLI table-backed SQL `run` success-path evidence was not found. Opened `.10x/tickets/2026-07-07-cli-sql-run-success.md`; this parent remains open for that final aggregate CLI-spine acceptance slice.
