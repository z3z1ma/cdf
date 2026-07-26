Status: done
Created: 2026-07-11
Updated: 2026-07-25
Parent: .10x/tickets/done/2026-07-11-p1-ws9-cli-experience-excellence.md
Depends-On: .10x/tickets/done/2026-07-11-p1-cx2-compact-renderer-errors.md, .10x/specs/runtime-event-spine.md

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

## Journal

- 2026-07-25: Implemented the live subscriber as a delivery mode of the existing neutral event
  sink. The binary selects live stderr; embedded/library invocations retain deterministic buffered
  capture. Runtime producers use `try_send` only and never wait for rendering.
- 2026-07-25: Interactive progress redraws no faster than 10 Hz and uses coalesced activity lines.
  Headless progress emits at most one ordinary milestone per phase and a terminal milestone, with
  plain line-oriented output on stderr. Normal final output suppresses the already-streamed
  history; verbose buffered/inspect paths retain detail.
- 2026-07-25: Boundedness review removed an unbounded seen-sequence set and replaced it with a
  capacity-bounded latest-sequence map. A full-suite failure then exposed that live headless state
  stopped advancing when the shared milestone buffer filled; live delivery now coalesces the
  oldest milestone while buffered capture preserves its drop policy.

## Evidence

- `.10x/evidence/2026-07-25-cli-experience-rewrite.md` records the complete commands, local
  benchmark, slow-terminal law, bounded-state tests, and real five-partition HTTPS-to-DuckDB run.
- Interactive refresh is fixed at 100 milliseconds; the terminal matrix and progress tests prove
  bounded active rendering, headless ANSI absence, terminal preservation, redaction, duplicate and
  out-of-order behavior, and recovery after failed terminals.
- The final full suite passed 320/320 after the live-coalescing correction.

## Review

Fresh-hat adversarial review found and corrected four significant lifecycle risks: unbounded
sequence memory, unbounded per-run headless coalescing state, a panic on worker spawn failure, and
false live-delivery suppression after a worker panic. Worker creation now falls back to buffered
delivery, a failed join leaves final progress renderable, and all subscriber state is bounded.

Verdict: pass. CX4 retains the hosted end-to-end overhead and canonical-recording gates.

## Retrospective

Buffered evidence capture and live presentation need different overflow behavior. Reusing
buffered “drop when full” semantics in the decoupled live worker was bounded but stale; explicit
coalescing preserves both the memory ceiling and the operator's view of current work. Terminal
events warrant their own one-slot path because ordinary progress is intentionally disposable.
