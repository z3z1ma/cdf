Status: active
Created: 2026-07-19
Updated: 2026-07-19

# Iceberg and AWS Glue source program

## Scope

Parent plan for first-class Iceberg v1/v2 Parquet sources over local/REST/Glue catalogs and conventional AWS Glue external-table sources, preserving CDF source extension, deterministic evidence, constant-memory, portable-worker, and P3 performance laws.

## Non-goals

This parent is not executable. It does not own Iceberg/Delta destinations, Glue ETL orchestration, catalog writes, or unrelated connector breadth.

## Acceptance Criteria

- Iceberg local filesystem, REST, and Glue catalog tables compile, discover, pin, plan, preview, run, replay, and doctor through one source driver.
- Snapshot identity, schema/partition evolution, position/equality deletes, time travel, and append-only ancestry are exact and artifact-backed.
- Conventional Glue external tables use shared format/object access with exact classification and Lake Formation authorization.
- Million-task planning remains bounded/spill-backed and jobs 1/N yields identical package identity.
- Local and authorized FQ12 conformance plus P3 performance evidence meet the governing specs.
- No generic project/runtime/engine/package/destination branch names Iceberg or Glue.

## Child graph

1. `.10x/tickets/2026-07-19-iceberg-f1-neutral-object-access.md`
2. `.10x/tickets/2026-07-19-iceberg-f2-arrow59-dependency.md`
3. `.10x/tickets/2026-07-19-iceberg-f3-table-snapshot-position.md`
4. `.10x/tickets/2026-07-19-iceberg-f4-externalized-scan-tasks.md`
5. `.10x/tickets/2026-07-19-iceberg-i1-catalog-discovery.md`
6. `.10x/tickets/2026-07-19-iceberg-i2-scan-execution.md`
7. `.10x/tickets/2026-07-19-iceberg-i3-incremental-product-conformance.md`
8. `.10x/tickets/2026-07-19-glue-g1-external-table-source.md`
9. `.10x/tickets/2026-07-19-glue-g2-lake-formation-live-conformance.md`

F1 and F2 are parallel. F3 is independent of F1/F2 but must avoid the active WX1 implementation files until that worker lands. F4 depends on WX1 and coordinates with P3 F2 rather than duplicating task/cardinality authority. I1 depends on F1/F2/F3. I2 depends on F4/I1. I3 depends on I2. Glue G1 depends on F1 and may proceed beside I1; G2 depends on G1 and the Iceberg Glue binding.

## References

- `.10x/decisions/iceberg-glue-source-boundaries.md`
- `.10x/specs/iceberg-source.md`
- `.10x/specs/aws-glue-external-table-source.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/specs/deterministic-parallel-scheduler.md`
- `.10x/specs/portable-partition-task-protocol.md`
- `.10x/decisions/arrow-datafusion-tuple-policy.md`
- `.10x/knowledge/cdf-product-objective.md`

## Assumptions

- User-ratified 2026-07-19: the architecture and semantics in the referenced decision/specs are approved for implementation.
- Record-backed: FQ12 credentials and AWS resources exist for later live testing, but external mutations still require a concrete confirmation at the time of provisioning.
- Record-backed: another worker currently owns WX1/runtime worker protocol; this lane must remain orthogonal.

## Journal

- 2026-07-19: Program shaped from code/source inspection and current Apache Iceberg/AWS Glue APIs. The worktree had one unrelated modification in `crates/cdf-runtime/src/worker_protocol.rs`; this program claimed the neutral object-access/source lane and will not edit that file.

## Blockers

Child-specific only. Parent closure depends on all children.

## Evidence

None yet; parent aggregates closed-child evidence.

## Review

Pending child closure.

## Retrospective

Pending.
