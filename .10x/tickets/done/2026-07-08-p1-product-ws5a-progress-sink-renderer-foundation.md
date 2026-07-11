Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-08-p1-product-ws5-live-progress.md
Depends-On: .10x/specs/cli-live-progress.md, .10x/tickets/done/2026-07-08-p1-product-ws1a-run-event-sink-foundation.md, .10x/tickets/done/2026-07-08-p1-product-ws3b-renderer-foundation.md, .10x/tickets/done/2026-07-08-p1-product-ws2c-product-grammar-semantics.md

# P1 product WS5A: Progress sink and renderer foundation

## Scope

Build the CLI live-progress subscriber and rendering primitives without wiring every command path.

Primary write scope is `crates/cdf-cli/src/**` progress modules, renderer integration points, parser/display flags needed for `-v` and `-q` if WS2C has established them, focused tests, and this ticket's records.

## Acceptance criteria

- A `RunEventSink` implementation converts run events into progress phases without blocking the run.
- Interactive and headless progress modes share one redaction boundary and renderer configuration.
- Verbose and quiet display modes are represented consistently with the grammar lane.
- The sink handles accepted, dropped, duplicate, out-of-order, and terminal events deterministically.
- Unit tests cover phase mapping, redaction, backpressure/drop behavior, headless formatting, and quiet/verbose behavior.

## Evidence expectations

Record focused `cdf-cli` tests, renderer snapshots for representative progress events, redaction adversarial output, and required scoped `QUALITY.md` checks, including jscpd and complexity reports.

## Explicit exclusions

Do not wire run/replay/resume/backfill commands end to end except for test doubles. Do not add NDJSON event streaming. Do not change runtime event semantics.

## Progress and notes

- 2026-07-08: Split from WS5 after creating `.10x/specs/cli-live-progress.md`.
- 2026-07-08: Implemented `crates/cdf-cli/src/progress.rs` and registered it in `crates/cdf-cli/src/lib.rs` as a dormant WS5A foundation module.
- 2026-07-08: Added `CliProgressSink` as a `RunEventSink` implementation using `try_lock` and bounded milestone buffering so progress cannot block runtime event publication. The sink maps run events to plan/extract/validate/package/commit/verify/gate phases, keeps `run_failed` on the current failed phase, records quiet/normal/verbose display modes, redacts `SecretRef` and URI userinfo before display, and renders headless milestone lines or interactive renderer primitives through shared `ProgressConfig`.
- 2026-07-08: Focused tests cover phase mapping for the full current `RunEventKind` vocabulary, redaction for headless and interactive rendering, nonblocking/backpressure drops, duplicate and out-of-order no-ops, terminal behavior, headless formatting, and quiet/verbose behavior.
- 2026-07-08: Verification evidence recorded in `.10x/evidence/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation.md`; closure review recorded in `.10x/reviews/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation-review.md`.
- 2026-07-08: Parent review hardened progress redaction with the shared renderer sensitive-key predicate, preserving the existing focused progress tests and formatter gate.

## Blockers

None. WS1A, WS2C, and WS3B are done.

## Evidence

- `.10x/evidence/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation.md`

## Review

- `.10x/reviews/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation-review.md`
