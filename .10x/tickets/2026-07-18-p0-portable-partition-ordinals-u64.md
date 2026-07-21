Status: open
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-18-p0-post-iceberg-integration-stabilization.md
Depends-On: .10x/tickets/done/2026-07-18-p0-external-partition-authority.md

# P0: make portable partition cardinality and ordinals u64

## Scope

Remove the artifact/protocol ceiling that rejects external task sets above `u32::MAX`. Portable partition counts and canonical partition/unit ordinals are `u64` from compiler authority through scheduling, retry evidence, worker tasks, segment assembly, and replay. Host-local collection indexes and active concurrency remain `usize` and use checked conversion only at the bounded materialization boundary.

## Non-goals

- Increasing active concurrency or memory admission.
- Changing segment artifact identifiers or row ordinals unrelated to portable partition scheduling.
- Materializing an entire high-cardinality external task set.

## Acceptance Criteria

- External task-set authority accepts counts above `u32::MAX` without enumeration.
- No portable partition/task ordinal narrows to `u32` or `usize` before bounded host scheduling.
- Worker task serialization and retry evidence round-trip ordinals above `u32::MAX`.
- Existing deterministic jobs-invariance and external-task tests remain green.

## Assumptions

- Record-backed: artifact cardinality is portable `u64`; host-local concurrency is `usize`.
- User-ratified: terabyte/petabyte scale must not carry a hard `u32` task ceiling.

## Journal

Pending activation.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
