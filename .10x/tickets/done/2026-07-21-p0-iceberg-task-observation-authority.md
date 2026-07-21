Status: done
Created: 2026-07-21
Updated: 2026-07-21
Parent: .10x/tickets/done/2026-07-21-p0-iceberg-execution-robustness.md

# P0: Iceberg external-task schema-observation authority

## Scope

Make the Iceberg external task reader attach the exact partition-scoped effective-schema observation
identity and binding required by the compiled stream-admission lifecycle.

## Non-goals

No weakening of generic observation uniqueness, no table-wide fake observation, no current-schema
pre-scan, and no Iceberg branch in kernel, runtime, project, engine, or CLI code.

## Acceptance Criteria

- Every decoded Iceberg external task has a unique stable observation ID and a canonical binding to
  its immutable partition plan.
- A real registry/execution lifecycle with a pinned effective schema accepts the task before source
  payload contact.
- Tampered, missing, or reused task observation authority still fails closed.

## References

- `.10x/specs/iceberg-source.md`
- `.10x/tickets/done/2026-07-13-p0-sa1-compiled-stream-admission-plan.md`
- `.10x/tickets/done/2026-07-19-iceberg-i2-scan-execution.md`

## Assumptions

- Record-backed: `cdf_kernel::partition_schema_observation_binding` is the canonical fallback binding
  for one planned partition and must be computed after the source-owned plan is complete.

## Journal

- 2026-07-21: Root cause confirmed in `IcebergPlannedPartitionReader::decode_task`: it writes only
  `cdf:external_task_sha256`, while the registry wrapper requires partition-scoped observation ID and
  binding whenever an effective schema runtime exists. Existing Iceberg tests passed `None` for that
  runtime and therefore encoded away the product lifecycle.
- 2026-07-21: The source-owned task decoder now records the stable partition id as the observation
  id after the complete immutable partition plan exists, derives the binding with the binding field
  absent, and validates the recorded value against an independent recomputation before returning
  the external task. The lifecycle test now crosses discovery into a pinned effective runtime and
  rejects a syntactically valid substituted binding instead of reading the stored value back.

## Blockers

None.

## Evidence

- `cargo test -p cdf-source-iceberg --lib`: all source tests passed after the lifecycle and tamper
  cases were added. The exact final verification command is recorded in the parent barrier.
- Fresh release `cdf run flolake.transactions --to parquet://idk.parquet` traversed all 84 external
  tasks and committed 3,513,266 rows. This proves the previously failing registry/execution path
  accepts the source-owned identities; it does not independently prove every possible tamper class.
- Integrated verification passed all 41 Iceberg tests plus the aggregate 733-test core suite,
  workspace compilation, and strict clippy.

## Review

Pass. The binding includes both immutable partition semantics and the external task-set authority;
missing, substituted, or cross-task observations fail before source payload admission. No generic
orchestration layer branches on Iceberg identity.

## Retrospective

Effective-schema observation metadata is part of executable-task authority, not optional runtime
decoration. Tests that omit the effective runtime can validate task decoding while completely
missing the product lifecycle that consumes it.
