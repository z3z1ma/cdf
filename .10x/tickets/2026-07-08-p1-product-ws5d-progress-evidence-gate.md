Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws5-live-progress.md
Depends-On: .10x/specs/cli-live-progress.md, .10x/tickets/done/2026-07-08-p1-product-ws5b-run-replay-resume-progress.md, .10x/tickets/2026-07-08-p1-product-ws5c-backfill-multi-resource-progress.md

# P1 product WS5D: Progress evidence gate

## Scope

Close the live-progress workstream with terminal recordings, headless snapshots, redaction proof, and migration checks after command wiring lands.

Primary write scope is snapshot/recording fixtures, test harnesses or scripts needed to reproduce them, evidence/review records, and minimal test updates. Avoid broad command behavior changes.

## Acceptance criteria

- Recorded TTY sessions or deterministic equivalents cover `run`, `replay package`, `resume`, and executed `backfill` success paths.
- Failure/chaos evidence covers failed phase, preserved artifacts, mutation status, and exact next command.
- Headless log snapshots cover all progress-enabled commands and contain no ANSI or spinner control sequences.
- Redaction adversarial tests cover event details, destination URIs, secrets, Python/interpreter outputs where applicable, and package paths where sensitive.
- A migration check proves progress-enabled commands do not bypass the progress/rendering layer in human mode.

## Evidence expectations

Record terminal-session artifacts under `.10x/evidence/.storage/` when binary or large, evidence records summarizing each run, final adversarial review, and the scoped quality gate output required by `QUALITY.md`.

## Explicit exclusions

Do not implement new progress behavior except to repair findings in the already scoped command paths. Do not add NDJSON event streaming.

## Progress and notes

- 2026-07-08: Split from WS5 to keep evidence closure separate from command wiring.

## Blockers

Blocked until WS5B and WS5C land.
