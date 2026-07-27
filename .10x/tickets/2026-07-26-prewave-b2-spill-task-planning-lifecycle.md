Status: open
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/done/2026-07-26-prewave-b1-typed-task-set-reader.md`

# Unify spill-backed task planning lifecycle

## Scope

Extract the common task-planning workspace lifecycle used by Iceberg and Glue: bounded resident
metadata, spill admission, canonical ordinal/content emission, task-set authority finalization,
cancellation cleanup, and publication into `ExternalTaskStore`. Keep index records and planning
algorithms source-owned.

## Non-goals

- No common Iceberg manifest/Glue partition model.
- No requirement that one task equal one object or file.
- No new task artifact format or compatibility reader.

## Acceptance criteria

- Both planners use one accounted builder/workspace lifecycle.
- Canonical task identity and publication are deterministic across spill thresholds and jobs.
- Cancellation, disk exhaustion, malformed records, and finalize failure leave no leaked leases or
  unpublished artifacts.
- Source-specific planning indexes retain their own typed records and selection logic.
- Existing high-cardinality planning tests pass with equal or lower peak control memory and no
  material throughput regression.

## References

- `.10x/specs/catalog-task-source-commons.md`
- `.10x/specs/deterministic-parallel-scheduler.md`

## Assumptions

- Source-backed: both planning indexes already use `ExternalTaskStore`,
  `ExternalTaskWorkspace`, shared spill coordination, and canonical task emission.

## Journal

- 2026-07-26: Scoped below source planning semantics; only the artifact/workspace lifecycle is
  shared.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
