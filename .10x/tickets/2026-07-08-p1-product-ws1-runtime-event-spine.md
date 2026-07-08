Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-experience-program.md
Depends-On: .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# P1 product WS1: Runtime event spine

## Scope

Add a first-class run event system in `cdf-project` and `cdf-kernel` that can feed the run ledger, CLI renderer, and tracing bridge without changing deterministic package/checkpoint behavior.

This workstream is broad enough to split before implementation if needed. The first executable child should define the kernel/project event model and non-blocking sink contract before wiring all runtime paths.

## Required outcomes

- Define a `RunEvent` stream emitted at every lifecycle boundary already named by the run ledger: run start, plan recorded, package start, segment/batch progress, package finalized, destination commit start, per-segment ack, receipt recorded, checkpoint proposed, checkpoint committed, run succeeded/failed/resumed, replay recorded, and validation-depth transition recorded.
- Add quantitative payloads where available: rows, bytes, batches, segments, elapsed display data, phase, quarantine counts, retry and backoff notices.
- `ProjectRunRequest` accepts a bounded, non-blocking event sink; a slow subscriber MUST NOT stall the run.
- The run-ledger writer becomes one subscriber, the CLI renderer can become another, and a tracing bridge emits run/resource/partition/package IDs as fields per VISION D-19.
- Backfill, replay, resume, and conformance live-runs emit through the same spine.
- OTLP export remains a feature-flag follow-up under the observability parent unless separately ratified.

## Acceptance criteria

- Event ordering is tested for successful and failing lifecycle paths.
- A non-blocking-sink stress test proves slow subscribers do not stall the run.
- Redaction checks cover event payloads before renderer consumption.
- Existing ledger events remain queryable and are not weakened by the live event path.

## Evidence expectations

Record focused tests, stress output, redaction adversarial output, and integration evidence showing at least one live run emits the expected ordered event stream. WS5 must later consume this spine end to end.

## Explicit exclusions

No CLI spinner/progress UI in this workstream except test doubles. No OTLP exporter. No package artifact identity changes.

## Progress and notes

- 2026-07-08: Opened from P1 product directive. This is the prerequisite for live progress and a durable observability bridge.
- 2026-07-08: Split executable child `.10x/tickets/done/2026-07-08-p1-product-ws1a-run-event-sink-foundation.md` for the shared event DTOs plus non-blocking live sink accepted by `ProjectRunRequest`.

## Blockers

None.
