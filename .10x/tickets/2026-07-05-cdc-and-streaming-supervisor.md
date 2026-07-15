Status: open
Created: 2026-07-05
Updated: 2026-07-14
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
- 2026-07-11: P3 now owns the reusable kernel stream-policy artifacts, policy compilation, deterministic drain epoch executor, and watermark/late-data conformance through BX1 and A7–A9. This ticket retains concrete log CDC source archetypes, `cdc_apply`, and resident pause/drain/resume lifecycle and MUST consume the P3 epoch executor rather than introduce a second runtime/artifact path. Split this broad parent into executable children before implementation.
- 2026-07-14: Resident non-pausable source replay MUST consume A8's bounded rolling-spool/checkpoint-eviction contract. An unbounded source cannot reuse the finite known-object growing spool, and no resident lifecycle may turn replay retention into unbounded disk growth.

## Blockers

None.
