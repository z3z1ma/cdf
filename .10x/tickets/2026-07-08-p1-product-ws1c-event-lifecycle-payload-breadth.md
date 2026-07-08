Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws1-runtime-event-spine.md
Depends-On: .10x/specs/runtime-event-spine.md, .10x/tickets/2026-07-08-p1-product-ws1b-event-fanout-subscriber-architecture.md

# P1 product WS1C: Event lifecycle and payload breadth

## Scope

Fill the remaining lifecycle and quantitative event gaps needed by P1 live progress, tracing, and conformance.

Primary write scope is `crates/cdf-kernel/src/run_event.rs`, `crates/cdf-project/src/runtime/**`, destination-session stage hooks if needed, focused tests, and this ticket's records.

## Acceptance criteria

- Event vocabulary covers segment or batch progress where available, package finalized, destination commit start, per-segment or commit acknowledgment where available, receipt recorded, checkpoint proposed/committed, run succeeded/failed/resumed, replay recorded, and validation-depth transition.
- Quantitative payloads are emitted when trustworthy values exist: rows, bytes, batches, segments, elapsed display data, phase, quarantine counts, retry and backoff notices.
- Missing totals are omitted or represented as unknown; code does not invent values.
- Redaction validation applies to all new details.
- Event ordering is tested for successful and failing lifecycle paths.

## Evidence expectations

Record focused runtime tests for new lifecycle events and payloads, failure-path ordering tests, redaction tests, and required scoped quality checks from `QUALITY.md`.

## Explicit exclusions

Do not implement CLI rendering or tracing bridge. Do not add retry behavior solely to create retry events.

## Progress and notes

- 2026-07-08: Split from WS1 because WS1A proved the foundation but did not add all quantitative payloads or lifecycle coverage named by P1.

## Blockers

Blocked until WS1B lands.
