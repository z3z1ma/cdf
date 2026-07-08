Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws1-runtime-event-spine.md
Depends-On: .10x/specs/runtime-event-spine.md, .10x/tickets/done/2026-07-08-p1-product-ws1a-run-event-sink-foundation.md

# P1 product WS1B: Event fanout subscriber architecture

## Scope

Refactor the WS1A recorder seam into an explicit event fanout where the durable run ledger is the mandatory subscriber and live sinks are non-authoritative subscribers.

Primary write scope is `crates/cdf-project/src/runtime/**`, `crates/cdf-kernel/src/run_event.rs` if the sink trait needs a narrow companion type, focused tests, and this ticket's records.

## Acceptance criteria

- Runtime event publication has an explicit fanout boundary rather than ad hoc ledger append plus optional sink calls.
- The SQLite run ledger remains mandatory and authoritative: ledger append failure fails the run.
- Non-authoritative sinks remain bounded/drop-capable and cannot fail or stall the run.
- Live subscribers receive persisted event envelopes after durable append.
- Existing WS1A ordering/drop/redaction tests still pass or are upgraded to the new boundary.
- Package hashes, receipts, checkpoints, and package statuses remain unchanged.

## Evidence expectations

Record focused runtime tests, sink-drop stress output, redaction tests, artifact identity assertions, and scoped `QUALITY.md` checks including jscpd, complexity reports, Semgrep, Gitleaks, and reusable CodeQL if Rust source changed.

## Explicit exclusions

Do not implement CLI progress rendering, tracing bridge, OTLP export, or new lifecycle payloads beyond what the refactor requires.

## Progress and notes

- 2026-07-08: Split from WS1 after WS1A closed. WS1A intentionally emitted to the optional sink after ledger append; this child makes the subscriber architecture explicit while preserving the durable-ledger authority.

## Blockers

None.
