Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws1-runtime-event-spine.md
Depends-On: .10x/specs/runtime-event-spine.md, .10x/tickets/2026-07-08-p1-product-ws1b-event-fanout-subscriber-architecture.md, .10x/tickets/2026-07-08-p1-product-ws1c-event-lifecycle-payload-breadth.md

# P1 product WS1D: Replay, resume, and backfill event spine

## Scope

Route replay, resume, backfill, and conformance live-run paths through the same event spine instead of command-specific or partial event handling.

Primary write scope is `crates/cdf-project/src/runtime/**`, `crates/cdf-cli/src/replay_command.rs`, `crates/cdf-cli/src/resume_command/**`, `crates/cdf-cli/src/backfill_command.rs`, conformance tests if needed, and this ticket's records.

## Acceptance criteria

- Package replay emits event-spine events for verification, destination write, receipt, checkpoint, duplicate/no-op, package status, success, and failure.
- Resume emits event-spine events for selected recovery action, source-contact status, replay/recover/repair actions, mutation status, success, and failure.
- Executed backfill emits event-spine events for each slice and preserves slice ordering.
- Conformance-owned live-run scenarios can observe event-spine output without SQLite-specific internals.
- Existing `inspect run` and run-ledger queries remain compatible.

## Evidence expectations

Record command/runtime tests, conformance observation evidence, no-source-contact resume evidence, duplicate replay evidence, failure-path evidence, and scoped `QUALITY.md` checks including jscpd and complexity reports.

## Explicit exclusions

Do not implement human progress rendering; WS5 consumes this. Do not change replay/resume semantics beyond event publication.

## Progress and notes

- 2026-07-08: Split from WS1 because WS1A excluded broader replay/resume/backfill event plumbing.

## Blockers

Blocked until WS1B and WS1C land.
