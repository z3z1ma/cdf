Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws1c-event-lifecycle-payload-breadth.md
Verdict: pass

# P1 product WS1C event lifecycle and payload breadth review

## Target

Implementation and evidence for `.10x/tickets/done/2026-07-08-p1-product-ws1c-event-lifecycle-payload-breadth.md`.

## Assumptions tested

- New event kinds must persist through the mandatory SQLite durable subscriber before live subscribers receive them.
- Segment progress and destination acknowledgments must use existing trusted package segment and `SegmentAck` data, not synthetic totals.
- Optional quantitative values must be omitted when absent.
- Redaction validation must still run before any live sink emission.
- Event ordering must remain deterministic on successful and failing lifecycle paths.
- Package identity, receipt identity, checkpoint authority, and destination commit semantics must not change.
- CLI rendering, tracing, OTLP export, and retry behavior remain excluded.

## Findings

No blocking findings.

The implementation uses the existing WS1B fanout and only expands the durable event append DTOs and stage-hook metadata needed to report existing facts. `package_segment_recorded` uses finalized `SegmentEntry` row/byte counts. `destination_segment_acknowledged` uses `CommitSession::write_segment` results. Commit-start segment count comes from the prepared commit request. Receipt details use receipt counts and omit optional count fields when `None`.

The SQLite schema bump to v3 is necessary because the durable ledger has a `kind IN (...)` check constraint. Migration rewrites the event table with the widened constraint while preserving existing rows and append-only triggers/indexes.

Two conformance helper matches needed no-op arms for the new public `RuntimeStage::DestinationSegmentAcknowledged`; this was compile fallout from the public enum change and does not change crash-window semantics.

Quality evidence is sufficient for this slice. CodeQL completed with 0 SARIF findings but continues to have Rust extractor macro warnings and partial extraction coverage, which is an existing tooling limitation rather than a WS1C finding.

## Verdict

Pass. WS1C satisfies the event lifecycle and payload breadth acceptance criteria without implementing CLI rendering, tracing bridge, OTLP export, new retry behavior, or destination/checkpoint/package semantic changes.

## Residual risk

Batch-level progress is represented through trustworthy package-level batch count and per-segment progress, not per-input-batch live events. The current engine does not expose a stable runtime hook for per-input-batch emission in this slice. Broader replay/resume/backfill vocabulary convergence remains a future WS1/WS5 surface.
