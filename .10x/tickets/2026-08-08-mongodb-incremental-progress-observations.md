Status: open
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

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
