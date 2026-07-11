Status: active
Created: 2026-07-08
Updated: 2026-07-08

# Runtime event spine

## Purpose and scope

This specification governs the runtime event spine used by the run ledger, live CLI progress, tracing, conformance observations, replay, resume, and backfill. It refines `.10x/specs/run-orchestration-ledger.md` for product-facing event publication without changing checkpoint authority.

It derives from `VISION.md` decisions D-19 and D-21; `.10x/specs/project-cli-observability-security.md`; `.10x/specs/run-orchestration-ledger.md`; `.10x/tickets/done/2026-07-08-p1-product-ws1a-run-event-sink-foundation.md`; and `.10x/tickets/done/2026-07-08-p1-product-ws1-runtime-event-spine.md`.

## Behavior

The run ledger remains the durable authority for run history. Live subscribers, renderers, tracing bridges, and test subscribers MUST NOT authorize state advancement or mutate package, destination, receipt, checkpoint, or replay artifacts.

Runtime event publication MUST use a fanout model with:

- one mandatory durable ledger subscriber,
- zero or more non-authoritative live subscribers,
- bounded/drop-capable behavior for non-authoritative subscribers,
- deterministic event ordering for lifecycle events.

If the durable ledger append fails, the run MUST fail according to existing run-spine rules. If a non-authoritative subscriber is full, slow, or unavailable, the event MAY be dropped and the run MUST continue.

Non-authoritative subscribers SHOULD receive the persisted event envelope after durable ledger append so they can display run id, sequence number, timestamp, pointers, kind, and details exactly as the ledger recorded them. If a future non-SQLite ledger changes how persistence envelopes are created, the event spine MUST preserve that observable contract.

The event spine MUST cover lifecycle events for:

- run start,
- plan recorded,
- package start,
- segment or batch progress where available,
- package finalized,
- validation depth transition,
- destination commit start,
- per-segment or commit acknowledgment where available,
- receipt recorded,
- checkpoint proposed,
- checkpoint committed,
- package status updated,
- run succeeded,
- run failed,
- run resumed,
- replay recorded.

Event details MUST use typed `RunEventValue` values. Secret references MUST use typed `SecretRef`; raw secret-looking strings are forbidden. Quantitative payloads MUST be emitted when the runtime has trustworthy values and omitted when it does not.

The tracing bridge MUST use the same event spine and emit run, resource, scope, partition, package, destination, plan, checkpoint, and receipt identifiers as structured fields. OTLP export remains a separate feature-gated follow-up.

## Acceptance criteria

- A successful live run produces durable and live events in the same order.
- A failing live run records and emits the failed lifecycle event without losing earlier durable events.
- Dropped non-authoritative subscriber events do not change run success, package identity, receipt verification, checkpoint commits, or ledger completeness.
- Replay, resume, backfill, and conformance live-run paths use the same event vocabulary or explicitly recorded exclusions.
- Redaction guardrails reject raw secret payloads before any live subscriber can receive them.
- Tracing bridge evidence shows structured fields without requiring an OTLP exporter.

## Explicit exclusions

This spec does not define CLI rendering, live progress layout, scheduler semantics, OTLP export, or a non-SQLite ledger backend. It does not make non-authoritative subscribers reliable delivery mechanisms.
