Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-08-p1-product-ws1-runtime-event-spine.md
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
- 2026-07-08: Worker started implementation. Inspected governing specs, WS1A/WS1B/WS1C tickets, WS1C evidence/review, engine tracing ticket, `QUALITY.md`, current runtime event modules/tests, and current git status. The only pre-existing dirty files are the unrelated WASM records called out by the handoff and are out of scope.
- 2026-07-08: Implemented `TracingRunEventSink` in `cdf-project`, emitted persisted runtime run events as structured `tracing` events, added strict tracing capture/redaction tests, added the direct `tracing` dependency edge for `cdf-project`, and avoided OTLP/CLI/event-vocabulary changes.
- 2026-07-08: Closure evidence recorded in `.10x/evidence/2026-07-08-p1-product-ws1e-tracing-bridge.md`; adversarial review recorded in `.10x/reviews/2026-07-08-p1-product-ws1e-tracing-bridge-review.md`.

## Blockers

None.
