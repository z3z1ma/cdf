Status: active
Created: 2026-08-08
Updated: 2026-08-08
Depends-On: `.10x/tickets/done/2026-08-06-u6b-default-live-telemetry.md`

# MongoDB incremental live progress observations

## Scope

Publish bounded process-local MongoDB extraction progress as admitted wire/decode batches complete
so the existing normal-mode live telemetry can show rows, bytes, batches, and phase-local rate
during a long read rather than only at final package publication.

## Non-goals

- durable per-batch run-ledger events;
- fabricated totals or row counts before decode;
- changes to package, checkpoint, receipt, or schema authority;
- connector-specific rendering.

## Acceptance Criteria

- Each completed MongoDB decode batch updates the existing drop-capable live progress sink with
  cumulative phase-local rows/bytes and explicit batch count.
- Observations never block extraction, alter backpressure, double-count packaging/destination
  counters, expose source values, or enter durable identity/evidence.
- TTY and headless tests prove counters advance before extraction completion; quiet, JSON, and
  disabled-progress behavior remain unchanged.
- One release Atlas run shows changing counters during extraction while retaining exact final rows,
  package hash, receipt, and checkpoint.

## References

- `.10x/specs/cli-live-progress.md`
- `.10x/tickets/done/2026-08-06-u6b-default-live-telemetry.md`
- `.10x/tickets/2026-08-02-mongodb-source-connector.md`

## Assumptions

- Record-backed: the 417,114-row Atlas portable run emitted elapsed-only heartbeats at 30, 60, and
  90 seconds even though the adapter had completed bounded wire/decode batches; final counters
  appeared only at extraction completion.

## Journal

- 2026-08-08: Opened from the final live Atlas portability certificate. The renderer and clock
  behavior are already correct; the missing authority is an adapter-to-process-local-progress
  observation at the completed decode-batch frontier.
- 2026-08-08: Activated. Source inspection found a stronger existing authority boundary than a
  MongoDB-specific callback: the engine validates and admits every source batch before downstream
  normalization. The repair publishes cumulative rows, bytes, and batches there through the
  existing drop-capable progress sink, improving all batch-producing adapters without durable
  events or connector-owned rendering.
- 2026-08-08: Implemented cumulative source-batch observations at the engine's validated batch
  frontier and mapped them into the existing extract phase. TTY redraw remains bounded, headless
  observations coalesce until the 30-second heartbeat, retry notices remain immediate, and the
  project recorder publishes no durable event. Focused tests and strict affected-package Clippy
  pass; the release Atlas visual certificate remains.

## Blockers

None.

## Evidence

- The focused engine test observed exact cumulative rows, bytes, and batch count for every source
  batch before package finalization.
- Two CLI-core tests prove cumulative replacement in normal TTY output and no per-batch headless
  emission before the bounded heartbeat, whose output contains the last-known counters.
- The project recorder test proves the observation reaches the process-local sink while the run
  ledger remains empty.
- The combined affected-package strict Clippy, formatter, and diff checks pass.

## Review

Pending.

## Retrospective

Pending.
