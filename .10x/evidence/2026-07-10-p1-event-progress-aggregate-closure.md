Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws1-runtime-event-spine.md, .10x/tickets/done/2026-07-08-p1-product-ws5-live-progress.md

# P1 runtime events and live progress aggregate closure

## What was observed

Run, replay, resume, and executed backfill all publish through the shared `RunEventSink`. Durable ledger append remains authoritative, live fanout is non-blocking, and the renderer consumes the same typed events for interactive and headless progress. Replay includes verification, destination, receipt, checkpoint, package-status, duplicate, success, and failure events. Resume publishes recovery action, source-contact, mutation, replay/repair, resumed, and failure facts. Backfill passes one sink through each ordered slice.

## Procedure

Fresh focused verification passed:

```text
cargo test -p cdf-project --locked general_project_run_live_sink --no-fail-fast
  2 passed
cargo test -p cdf-project --locked project_run_recorder_ --no-fail-fast
  2 passed
cargo test -p cdf-cli --locked replay_package_failure_ --no-fail-fast
  2 passed
cargo test -p cdf-cli --locked resume_finalized_package_human_progress_replays_without_source_contact --no-fail-fast
  1 passed
cargo test -p cdf-cli --locked progress::tests --no-fail-fast
  11 passed
cargo test -p cdf-cli --locked backfill_execute_ --no-fail-fast
  3 passed
```

The focused set proves durable/live order equality, dropped-live-event isolation, durable-append failure isolation, raw-secret rejection before emit, replay failure progress, no-source-contact resume progress, ordered multi-slice progress, failure recovery guidance, non-ANSI headless output, deterministic duplicate/out-of-order handling, bounded backpressure, terminal-event retention, quiet/verbose behavior, and redaction before either renderer.

Child evidence adds success-path lifecycle breadth, tracing fields, rich/headless renderer snapshots, chaos context, and migration-gate source scans under `.10x/evidence/2026-07-08-p1-product-ws1*.md` and `.10x/evidence/2026-07-08-p1-product-ws5*.md`. Scanner-residual closure is `.10x/evidence/2026-07-10-p1-ws5e-codeql-backfill-fixtures.md`.

## What this supports

Every acceptance criterion in WS1, WS1D, WS1F, WS5, and WS5D has implementation and test evidence. The deterministic rich/headless test harness is the reproducible equivalent of a terminal recording and avoids environment-specific escape-sequence artifacts.

## Limits

OTLP export and JSON event streaming remain explicitly excluded. This does not claim P3 parallel-runtime progress behavior.
