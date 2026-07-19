Status: blocked
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md
Depends-On: .10x/tickets/2026-07-11-p0-wx1-portable-partition-task-protocol.md, .10x/tickets/2026-07-11-p3-f2-materialization-closure-audit.md

# Iceberg F4: externalized canonical scan-task sets

## Scope

Implement source-neutral content-addressed, bounded/spill-backed planned partition/task sets and specialize a safe canonical Iceberg task payload without serializing upstream Iceberg task structs.

## Non-goals

No remote scheduler/RPC, scan reader, catalog implementation, secret material, or unbounded inline `Vec` fallback.

## Acceptance Criteria

- Million-task synthetic planning holds the configured metadata budget and deterministic order.
- Task artifacts are canonical, tamper-detecting, generation-bound, stream-readable, and portable-capsule compatible.
- Iceberg task payload contains complete data/delete/schema/spec/name-map/predicate authority and no credentials/plaintext key material.
- Jobs/timing/spill location cannot change task or final package identity.

## References

- `.10x/specs/iceberg-source.md`
- `.10x/specs/portable-partition-task-protocol.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/specs/deterministic-parallel-scheduler.md`

## Assumptions

- User-ratified 2026-07-19: externalized source-neutral task authority is required; no Iceberg-only task store.

## Journal

None yet.

## Blockers

WX1 canonical task/result types and P3 F2 metadata-cardinality authority must land first or explicitly expose their integration seam.

## Evidence

Pending execution.

## Review

Pending.

## Retrospective

Pending.
