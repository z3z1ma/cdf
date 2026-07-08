Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws5-live-progress.md
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

## Blockers

None. WS1A, WS2C, and WS3B are done.
