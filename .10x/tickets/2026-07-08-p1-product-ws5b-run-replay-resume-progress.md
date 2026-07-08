Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws5-live-progress.md
Depends-On: .10x/specs/cli-live-progress.md, .10x/tickets/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation.md, .10x/tickets/done/2026-07-08-p1-product-ws3c-plan-run-rendering.md

# P1 product WS5B: Run, replay, and resume progress

## Scope

Wire live progress into `cdf run`, `cdf replay package`, and `cdf resume`.

Primary write scope is `crates/cdf-cli/src/run_command.rs`, `crates/cdf-cli/src/replay_command.rs`, `crates/cdf-cli/src/resume_command/**`, progress modules, focused tests, and this ticket's records. Touch `crates/cdf-project/src/runtime/**` only if replay/resume need a narrow event-sink seam that cannot be expressed in the CLI layer.

## Acceptance criteria

- `cdf run` passes the progress sink to `ProjectRunRequest::event_sink` in human mode.
- `cdf replay package` emits or subscribes to progress-equivalent run events for package verification, destination write, receipt verification, checkpoint gate, duplicate handling, and failure.
- `cdf resume` shows recovery progress from existing ledger events and newly appended recovery events without contacting the source after package finalization.
- `--json` output remains stable and does not interleave human progress.
- TTY-rich and headless snapshots or recordings cover success, failure, duplicate replay, and post-finalization resume.
- Redaction covers destination URIs, package paths, and event details.

## Evidence expectations

Record command tests, TTY/headless snapshots or terminal recordings, chaos-path output evidence, JSON compatibility tests, redaction adversarial output, and scoped `QUALITY.md` checks including jscpd and complexity reports.

## Explicit exclusions

Do not wire backfill. Do not add NDJSON event streaming. Do not redesign final non-progress panels beyond dependencies already landed by WS3.

## Progress and notes

- 2026-07-08: Split from WS5. Source inspection found `run_command.rs` already accepts `event_sink: None`; replay and resume append ledger events through command-specific paths and may need narrow sink seams.

## Blockers

Blocked until WS5A and WS3C land.
