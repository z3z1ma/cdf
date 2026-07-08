Status: open
Created: 2026-07-05
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md, .10x/tickets/done/2026-07-05-datafusion-engine-planner.md, .10x/tickets/2026-07-05-conformance-chaos-golden.md, .10x/tickets/done/2026-07-07-p0-workstream-a-streaming-commit-session.md, .10x/tickets/done/2026-07-07-p0-workstream-b-open-orchestrator-world.md, .10x/tickets/done/2026-07-07-p0-workstream-c-spine-conformance-harness.md

# Implement CDC and streaming supervisor

## Scope

Implement log-based CDC source archetypes, `LogPosition` transaction-boundary semantics, `cdc_apply` disposition support, unbounded resident streaming supervisor, checkpoint cadence, package rotation, watermarks, drain/pause/resume lifecycle, and related conformance scenarios.

## Acceptance criteria

- CDC batches carry `_cdf_op` and ordered source positions.
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
- 2026-07-07: Blocked by the P0 structural-debt stop-line until Workstreams A, B, and C close. This ticket owns new CDC source archetypes and the resident streaming supervisor, both explicitly paused by the directive.
- 2026-07-08: Workstream B closed. This ticket remains blocked by the P0 stop-line until Workstream C closes.
- 2026-07-08: Workstream C closed at `.10x/tickets/done/2026-07-07-p0-workstream-c-spine-conformance-harness.md`; the A-C stop-line is lifted for new source-archetype and resident streaming-supervisor lanes. This ticket is open again, though broader P0 Workstreams E and F remain the current structural-debt priority.

## Blockers

None.
