Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p1-ws9-cli-experience-excellence.md
Depends-On: .10x/tickets/2026-07-11-p1-cx2-compact-renderer-errors.md, .10x/specs/runtime-event-spine.md

# P1 CX3: live progress activity model

## Scope

Replace milestone-history dumps with rate-limited/coalesced phase and partition activity, stable final summaries, and clean headless stderr milestones for run/replay/resume/backfill.

## Acceptance criteria

- Interactive progress refreshes at no more than 10 Hz and uses bounded active lines.
- Normal final output does not repeat full history; verbose/inspect retain access.
- Headless progress is bounded, deterministic, plain, and on stderr.
- Dropped/out-of-order/terminal/redaction laws remain intact.

## Blockers

Depends on CX2.

## Evidence expectations

Recorded interactive and headless sessions, multi-partition/dropped-event/redaction tests, bounded-memory/rate assertions, and run/replay/resume/backfill parity snapshots.

## References

- `.10x/specs/cli-live-progress.md`
- `.10x/specs/runtime-event-spine.md`
