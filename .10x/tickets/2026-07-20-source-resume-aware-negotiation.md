Status: open
Created: 2026-07-20
Updated: 2026-07-20

# Source-neutral resume-aware negotiation

## Scope

Move committed-frontier binding ahead of expensive source task planning through a source-neutral
`QueryableResource` negotiation seam. Iceberg `append_snapshots` must prove ancestry before
loading manifests and plan only admitted append manifests; an unchanged snapshot must avoid
manifest-list/task-set planning entirely. Preserve the existing post-plan rebind seam only for
cheap inline partition rebinding if it remains necessary after the migration.

## Non-goals

No Iceberg/Glue identifiers in generic engine/project/CLI code, no checkpoint-store access from a
source adapter, no weakening of plan/package identity, and no source-owned state store.

## Acceptance Criteria

- The engine planner accepts an optional typed committed frontier and invokes one source-neutral
  resume negotiation method before task authority is materialized.
- Project run and plan commands obtain the applicable frontier through ordinary state authority;
  preview without a state binding remains explicit.
- Unchanged Iceberg runs do not open manifest lists or manifests; append runs load only manifests
  added by admitted append snapshots.
- File-manifest and ordinary partition resume laws remain green; no source kind appears in generic
  orchestration.
- Before/after local and FQ12 evidence records planning wall time, metadata objects/bytes, and
  task-store peak memory.

## References

- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/specs/iceberg-source.md`
- `.10x/tickets/done/2026-07-19-iceberg-i2-scan-execution.md`
- `.10x/tickets/done/2026-07-19-iceberg-i3-incremental-product-conformance.md`

## Assumptions

- Record-backed: committed state belongs to project/checkpoint authority, while source-specific
  frontier interpretation belongs behind `QueryableResource`.

## Journal

- 2026-07-20: I3 proved exact append filtering through the existing post-plan rebind seam. Live
  unchanged-snapshot evidence still spent 3.44 seconds in full task planning before clearing the
  scan, and the filtered append path necessarily materializes the full current task set first.
  This ticket owns that source-neutral lifecycle/performance debt; it is not an Iceberg correctness
  blocker and must not be solved by injecting state access or Iceberg branches into generic code.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
