Status: open
Created: 2026-07-05
Updated: 2026-07-05
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/2026-07-05-checkpoint-store-sqlite.md, .10x/tickets/2026-07-05-datafusion-engine-planner.md, .10x/tickets/2026-07-05-conformance-chaos-golden.md

# Implement CDC and streaming supervisor

## Scope

Implement log-based CDC source archetypes, `LogPosition` transaction-boundary semantics, `cdc_apply` disposition support, unbounded resident streaming supervisor, checkpoint cadence, package rotation, watermarks, drain/pause/resume lifecycle, and related conformance scenarios.

## Acceptance criteria

- CDC batches carry `_firn_op` and ordered source positions.
- `cdc_apply` commits inserts/updates/deletes effectively-once per position where destination sheet supports required idempotency.
- Streaming supervisor runs unbounded plans continuously over existing batch/package/checkpoint types.
- Pause, drain, and resume-from-head do not advance cursors ahead of receipts.
- Late data and watermark policies are enforced according to plan.

## Evidence expectations

Record CDC integration tests, streaming lifecycle tests, chaos recovery during streaming, and guarantee-table tests for `cdc_apply`.

## Explicit exclusions

No distributed scheduler in this ticket.

## Progress and notes

- 2026-07-05: Opened from book and specs.

## Blockers

None.

