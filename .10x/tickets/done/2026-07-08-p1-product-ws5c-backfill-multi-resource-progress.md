Status: done
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
- 2026-07-08: Implemented executed backfill progress by creating the WS5A/WS5B human progress sink in `backfill_command.rs`, passing it to every slice's `run_project` request, and attaching the final snapshot to human success and failure output only.
- 2026-07-08: Fixed multi-slice progress in `progress.rs` by tracking event ordering by `(run_id, sequence)` and resetting terminal state when a new run id starts. This prevents later backfill slices from being treated as duplicate or after-terminal events when their ledger sequences restart at 1.
- 2026-07-08: Added headless progress fields for `run` and `scope`, including window scopes, so slice lines are distinguishable in CI logs.
- 2026-07-08: Added a backfill summary footer for executed human output with succeeded slice count, total rows, and total segments.
- 2026-07-08: Added failure annotation for backfill slices, including failed slice window, planned package/checkpoint artifacts, mutation status, and `cdf resume <run_id>` when the progress snapshot has a recorded run id. The covered duplicate-package failure occurs before run-ledger creation and correctly reports that no recovery command is available yet.
- 2026-07-08: Added focused tests for multi-run progress sequence restarts, JSON backfill progress separation, executed multi-slice human backfill progress and summary output, and backfill pre-run failure rendering.
- 2026-07-08: Reduced WS5C-added test duplication with a shared live-Postgres SQL source helper after jscpd surfaced avoidable backfill setup clones.
- 2026-07-08: Parent review repaired recovery guidance to match run ids by the failing slice package id instead of using the latest progress event globally; added focused package-specific progress lookup coverage and reran the focused/full/static/supply-chain checks.
- 2026-07-08: Evidence recorded in `.10x/evidence/2026-07-08-p1-product-ws5c-backfill-multi-resource-progress.md`; review recorded in `.10x/reviews/2026-07-08-p1-product-ws5c-backfill-multi-resource-progress-review.md`.

## Blockers

None. WS5A and WS3D are done.

## Evidence

- `.10x/evidence/2026-07-08-p1-product-ws5c-backfill-multi-resource-progress.md`

## Review

- `.10x/reviews/2026-07-08-p1-product-ws5c-backfill-multi-resource-progress-review.md`
