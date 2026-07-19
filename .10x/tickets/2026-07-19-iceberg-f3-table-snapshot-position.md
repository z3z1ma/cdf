Status: open
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md

# Iceberg F3: typed table-snapshot position

## Scope

Add the source-neutral typed table-snapshot position and its aggregation, package/checkpoint, replay/promotion, state rendering, serialization, tamper, and property conformance.

## Non-goals

No Iceberg catalog/scan code, file identity replacement, migration CLI, runtime worker-protocol edits while WX1 is dirty, or legacy state compatibility.

## Acceptance Criteria

- Position fields bind protocol, catalog/table/ref, snapshot/sequence/parent, and metadata generation with canonical validation.
- Identical partition snapshots aggregate only after complete authority; divergent snapshots fail.
- Batch slicing, package/checkpoint, replay/promotion, state/inspect, canonical JSON/hash, tamper, and property fixtures agree.
- Current state version becomes the only state shape; no migration shim is introduced for nonexistent customers.

## References

- `.10x/decisions/iceberg-glue-source-boundaries.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/iceberg-source.md`

## Assumptions

- User-ratified 2026-07-19: typed snapshot authority replaces semantic workarounds.

## Journal

None yet.

## Blockers

Coordinate after the active WX1 worker lands to avoid overlapping exhaustive position/task protocol edits.

## Evidence

Pending execution.

## Review

Pending.

## Retrospective

Pending.
