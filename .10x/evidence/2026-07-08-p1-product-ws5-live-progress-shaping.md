Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws5-live-progress.md, .10x/specs/cli-live-progress.md

# P1 WS5 live progress shaping evidence

## What was observed

WS5 was broad and blocked on both the runtime event spine and the renderer. WS1A is now done and provides a non-blocking `RunEventSink`, shared `RunEvent` DTOs, and `ProjectRunRequest::event_sink`.

Source inspection found:

- `crates/cdf-kernel/src/run_event.rs` defines the shared event kinds, event details, secret reference value type, and non-blocking sink contract.
- `crates/cdf-project/src/runtime/ledger.rs` emits durable events to the live sink after ledger append.
- `crates/cdf-cli/src/run_command.rs` currently invokes `run_project` with `event_sink: None`.
- `crates/cdf-cli/src/backfill_command.rs` currently executes each backfill slice with `event_sink: None`.
- Replay and resume command paths append run-ledger events through command-specific code rather than a single `ProjectRunRequest` path.

## Procedure

Read the WS5 parent, WS1A done ticket, renderer decision, run event source, project runtime ledger source, and command entry points for run/backfill/replay/resume. Then wrote:

- `.10x/specs/cli-live-progress.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws5b-run-replay-resume-progress.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws5c-backfill-multi-resource-progress.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws5d-progress-evidence-gate.md`

## What this supports or challenges

This supports executing live progress after WS3 renderer and WS2 verbosity grammar land. It also records the current limitation that some quantitative event payloads are not yet emitted and must be added by child tickets when needed rather than invented in the renderer.

## Limits

No implementation, snapshots, recordings, or quality gates were run for this shaping slice. Child tickets own execution evidence.
