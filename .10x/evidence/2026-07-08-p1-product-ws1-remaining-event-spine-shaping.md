Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws1-runtime-event-spine.md, .10x/specs/runtime-event-spine.md

# P1 WS1 remaining event-spine shaping evidence

## What was observed

WS1A closed the first runtime event foundation: kernel-owned event DTOs, a non-blocking `RunEventSink`, optional `ProjectRunRequest::event_sink`, live emission after durable SQLite ledger append, ordering/drop/redaction tests, and parent quality evidence.

The WS1 parent still requires work not owned by WS1A:

- explicit subscriber fanout where the durable ledger is one subscriber and live sinks/tracing are separate non-authoritative subscribers;
- segment/batch progress, per-segment or commit acknowledgment, and quantitative payload breadth;
- replay, resume, backfill, and conformance live-run event-spine coverage;
- tracing bridge fields;
- aggregate evidence across success, failure, stress, and redaction paths.

Source inspection found the current event definitions in `crates/cdf-kernel/src/run_event.rs`, live emission in `crates/cdf-project/src/runtime/ledger.rs`, `event_sink: None` call sites in `run_command.rs` and `backfill_command.rs`, and command-specific replay/resume event append code under `crates/cdf-cli/src/replay_command.rs` and `crates/cdf-cli/src/resume_command/**`.

## Procedure

Read the WS1 parent, WS1A done ticket, WS1A evidence/review, event source files, and replay/resume/backfill command entry points. Then wrote:

- `.10x/specs/runtime-event-spine.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws1b-event-fanout-subscriber-architecture.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws1c-event-lifecycle-payload-breadth.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws1d-replay-resume-backfill-event-spine.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws1e-tracing-bridge.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws1f-event-spine-evidence-gate.md`

## What this supports or challenges

This supports continuing WS1 without treating WS1A as the entire event spine. It keeps CLI progress in WS5 while preserving WS1 ownership of event publication, payloads, fanout, tracing, and cross-runtime path coverage.

## Limits

No implementation, tests, or quality gates were run for this shaping slice. Child tickets own execution evidence.
