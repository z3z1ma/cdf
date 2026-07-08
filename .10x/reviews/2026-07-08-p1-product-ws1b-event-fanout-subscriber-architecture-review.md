Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws1b-event-fanout-subscriber-architecture.md
Verdict: pass

# P1 product WS1B event fanout subscriber architecture review

## Target

Implementation and evidence for `.10x/tickets/done/2026-07-08-p1-product-ws1b-event-fanout-subscriber-architecture.md`.

## Assumptions tested

- The SQLite run ledger must remain the mandatory durable subscriber.
- Durable append failure must fail publication before any live subscriber receives an event.
- Live sinks are non-authoritative and must not affect run success when full or dropping.
- Live sinks must receive the persisted event envelope, not a pre-persistence append DTO.
- The refactor must preserve WS1A ordering, drop, and redaction behavior.
- Package hashes, receipts, checkpoints, and package statuses must not drift.
- CLI renderer, tracing, OTLP, and new lifecycle payload work remain excluded.

## Findings

No blocking findings.

The new `RunEventFanout` boundary is explicit and narrow. It names the durable SQLite subscriber separately from live subscribers while preserving the existing public `ProjectRunRequest::event_sink` surface. The durable subscriber is called first and returns `Result`; live subscriber return values are ignored after persistence. A recorder-level test proves missing durable run state prevents live emission, and runtime tests prove persisted-before-live delivery plus all-dropped sink behavior.

The artifact identity assertions are appropriately test-level: they check package manifest hash, receipt package hash, checkpoint delta package hash, and package lifecycle status without changing artifact creation code.

Quality evidence is sufficient for this slice. CodeQL's wrapper script had process-handling noise, but a direct analyze run against the refreshed database exited 0 with 0 SARIF results. CodeQL reports extraction errors in the broader Rust database; that is a tooling coverage limit, not a WS1B implementation finding.

Parent review reran fmt, focused fanout/redaction/drop/order tests, full `cdf-project` tests, clippy across `cdf-project`/`cdf-kernel`/`cdf-state-sqlite`, scoped diff whitespace checks, and direct unsafe/FFI token scanning. No parent-observed issue blocks closure.

## Verdict

Pass. WS1B satisfies the runtime event fanout architecture acceptance criteria without changing kernel event DTOs, CLI rendering, lifecycle vocabulary, package artifact identity, receipts, checkpoints, or package status semantics.

## Residual risk

The public request API still accepts one optional live sink. Internally it is represented as a subscriber list, which is enough for WS1B and avoids unnecessary public API expansion. Future multi-subscriber CLI/tracing work should add public composition only when a concrete caller needs it.
