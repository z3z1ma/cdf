Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws5-live-progress.md
Depends-On: .10x/specs/cli-live-progress.md, .10x/tickets/done/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation.md, .10x/tickets/done/2026-07-08-p1-product-ws3d-recovery-state-backfill-rendering.md

# P1 product WS5C: Backfill and multi-slice progress

## Scope

Wire live progress into executed `cdf backfill` and any multi-slice or multi-resource run loop that exists by the time this ticket starts.

Primary write scope is `crates/cdf-cli/src/backfill_command.rs`, progress modules, focused tests, and this ticket's records.

## Acceptance criteria

- Executed backfill attaches progress to each `run_project` slice.
- Interactive mode renders one line per slice or resource and a summary footer.
- Headless mode emits bounded milestone logs for each slice without ANSI or terminal control sequences.
- Failures show the failed slice, durable artifacts, mutation status, and exact next recovery command where applicable.
- The progress sink remains non-authoritative: dropped events do not change backfill success, package identity, ledger completeness, or checkpoint commits.

## Evidence expectations

Record backfill command tests, TTY/headless snapshots or recordings, sink-drop/backpressure tests, redaction evidence, and scoped `QUALITY.md` checks including jscpd and complexity reports.

## Explicit exclusions

Do not implement a scheduler or `run --loop`. Do not add new backfill planning semantics. Do not add NDJSON event streaming.

## Progress and notes

- 2026-07-08: Split from WS5. Current `backfill_command.rs` executes one `run_project` call per slice with `event_sink: None`; this child owns replacing that with the progress subscriber.

## Blockers

Blocked until WS5A and WS3D land.
