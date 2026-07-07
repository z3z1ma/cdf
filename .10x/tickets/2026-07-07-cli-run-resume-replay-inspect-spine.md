Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-run-spine-implementation-program.md
Depends-On: .10x/tickets/done/2026-07-07-general-run-orchestrator.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# Wire CLI run, resume, replay, and inspect-run to the spine

## Scope

Parent plan for wiring the existing CLI command surface to the general run spine now that the lower-layer orchestrator exists.

Children:

- `.10x/tickets/2026-07-07-cli-run-general-runtime.md`
- `.10x/tickets/2026-07-07-cli-replay-package-spine.md`
- `.10x/tickets/2026-07-07-cli-resume-spine.md`
- `.10x/tickets/2026-07-07-cli-inspect-run-spine.md`

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
- 2026-07-07: Split this broad ticket into executable children for `run`, `replay package`, `resume`, and `inspect run`. The first implementable child is `.10x/tickets/2026-07-07-cli-run-general-runtime.md`; `resume` is intentionally sequenced after run/replay destination parsing.
