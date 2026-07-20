Status: open
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md
Depends-On: .10x/tickets/done/2026-07-19-iceberg-f4-externalized-scan-tasks.md, .10x/tickets/done/2026-07-19-iceberg-i1-catalog-discovery.md

# Iceberg I2: bounded scan execution and delete semantics

## Scope

Execute canonical Iceberg tasks through the neutral object-access-backed Iceberg storage bridge and aligned reader, emitting preaccounted Arrow batches with exact projection, evolution, partition constants/defaults, position/equality deletes, pruning fidelity, retries, cancellation, and deterministic parallelism.

## Non-goals

No append ancestry/changelog/tailing, ORC/Avro/v3/encryption enablement, destination or generic runtime branch, or independent object/runtime/memory pool.

## Acceptance Criteria

- Iceberg v1/v2 Parquet scans and both delete forms match reference results across schema/partition evolution.
- Every retained payload is ledger-owned; no whole-table/file/task collection; too-small memory/disk fails cleanly.
- Jobs 1/N, retries, skew, cancellation, and generation drift preserve deterministic package/position/verdict authority.
- Local filesystem/REST performance scales to the actual CDF Parquet/CPU/device roofline and records phase evidence.

## References

- `.10x/specs/iceberg-source.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/specs/deterministic-parallel-scheduler.md`

## Assumptions

- User-ratified 2026-07-19: official aligned Iceberg reader is primary until evidence justifies replacing a hot stage.

## Journal

None yet.

## Blockers

None.

## Evidence

Pending execution.

## Review

Pending.

## Retrospective

Pending.
