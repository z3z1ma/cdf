Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws1-runtime-event-spine.md
Depends-On: .10x/specs/runtime-event-spine.md, .10x/tickets/done/2026-07-08-p1-product-ws1c-event-lifecycle-payload-breadth.md, .10x/tickets/2026-07-08-p1-product-ws1d-replay-resume-backfill-event-spine.md, .10x/tickets/2026-07-08-p1-product-ws1e-tracing-bridge.md

# P1 product WS1F: Event spine evidence gate

## Scope

Close WS1 with aggregate ordering, failure, stress, redaction, and tracing evidence across run, replay, resume, backfill, and conformance live paths.

Primary write scope is tests/evidence records and minimal harness repairs required to prove the WS1 acceptance criteria.

## Acceptance criteria

- Event ordering is tested for successful and failing lifecycle paths across the WS1-owned command/runtime surfaces.
- A non-blocking-sink stress test proves slow or full live subscribers do not stall the run.
- Redaction checks cover all event payload families before renderer/tracing consumption.
- Existing ledger events remain queryable and are not weakened by live event fanout.
- Aggregate evidence maps every WS1 parent acceptance criterion.

## Evidence expectations

Record aggregate evidence, adversarial review, conformance output where applicable, and scoped `QUALITY.md` checks. Include jscpd and complexity reports for any touched source.

## Explicit exclusions

Do not implement new product behavior except to repair WS1 evidence findings. CLI progress closure remains WS5.

## Progress and notes

- 2026-07-08: Split from WS1 to keep final evidence closure separate from event-spine implementation slices.

## Blockers

Blocked until WS1D and WS1E land.
