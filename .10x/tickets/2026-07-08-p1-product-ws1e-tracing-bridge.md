Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws1-runtime-event-spine.md
Depends-On: .10x/specs/runtime-event-spine.md, .10x/tickets/done/2026-07-08-p1-product-ws1b-event-fanout-subscriber-architecture.md, .10x/tickets/done/2026-07-06-engine-execution-tracing-spans.md

# P1 product WS1E: Tracing bridge

## Scope

Bridge runtime events into structured `tracing` fields without adding an OTLP exporter.

Primary write scope is runtime event/tracing modules in `crates/cdf-project/src/**` or `crates/cdf-cli/src/**` as appropriate, focused tests, and this ticket's records.

## Acceptance criteria

- Run events can be emitted as structured tracing events or spans with run, resource, scope, partition, package, destination, plan, checkpoint, receipt, event kind, and sequence fields.
- Tracing output preserves redaction guardrails for event details.
- The bridge is optional and does not affect package identity, run success, ledger completeness, or CLI JSON output.
- OTLP export remains an explicit follow-up and is not implemented here.

## Evidence expectations

Record tracing-capture tests, redaction tests, no-artifact-drift checks, and required scoped quality checks from `QUALITY.md`.

## Explicit exclusions

No OTLP exporter, no CLI progress renderer, and no new event vocabulary unless WS1C has already landed or explicitly owns the required addition.

## Progress and notes

- 2026-07-08: Split from WS1. Existing `.10x/tickets/done/2026-07-06-engine-execution-tracing-spans.md` proves engine tracing spans; this child bridges runtime run events to tracing.

## Blockers

None.
