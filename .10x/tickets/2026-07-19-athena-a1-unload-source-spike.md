Status: active
Created: 2026-07-19
Updated: 2026-07-20
Depends-On: .10x/tickets/done/2026-07-19-iceberg-i2-scan-execution.md

# Athena A1: UNLOAD source protocol and FQ12 roofline spike

## Scope

Falsify and shape a first-class AWS Athena source whose primary data plane submits a governed query, waits through an explicit cancellable execution lifecycle, freezes the returned result manifest, and feeds Athena `UNLOAD` Parquet objects into CDF's existing object-access, Parquet, schema-reconciliation, package, and checkpoint pipeline. Compare it directly with CDF's first-class Glue/Iceberg path over the same FQ12 table and predicate/projection.

## Non-goals

No generic Trino source disguised as Athena, no Athena destination, no credentials or signed URLs in artifacts, no SQL reparsing during execution/replay, no permanent benchmark objects, and no implementation before the protocol/spec decisions exposed by this spike are ratified.

## Acceptance Criteria

- Record the current Athena API and `UNLOAD` contract: supported output formats/compression, partition/file/result manifests, schema/type behavior, empty results, query idempotency, cancellation, timeout/retry, workgroup enforcement, encryption, result retention, and cleanup authority.
- Define a CDF-owned immutable query-execution position and portable partition task shape that contains query/result identities and object generations but no credentials; replay never resubmits SQL when the recorded result set is still valid.
- Prove that emitted Parquet partitions reuse the neutral object-access and Parquet execution seams with no Athena branch in generic runtime, engine, package, or destination code.
- On a separately confirmed FQ12 workgroup/result location, benchmark the same governed scan through direct Iceberg and Athena `UNLOAD`; report planning latency, bytes scanned/billed, result bytes, network throughput, decode/package throughput, peak memory/disk, and cleanup evidence.
- Produce the focused Athena source spec and bounded implementation tickets only if the spike retains the direction.

## References

- `.10x/knowledge/cdf-product-objective.md`
- `.10x/specs/iceberg-source.md`
- `.10x/specs/source-extension-runtime-contract.md`

## Assumptions

- User-ratified 2026-07-19: Athena is the next source after the Iceberg/Glue source reaches its measured roofline; `UNLOAD` Parquet is the primary hypothesis and FQ12 is the live comparison environment.
- User-ratified 2026-07-20: `UNLOAD` runs as a generic run-start partition materializer; replace is the default and append requires explicit disjoint cursor/window parameter authority; replay never resubmits SQL.
- User-ratified 2026-07-20: the source requires an explicit workgroup plus an enforced compatible scan cutoff or explicit `allow_unbounded_scan`; compression is configurable and Snappy is the provisional default pending the Snappy/ZSTD-1 sweep.
- User-ratified 2026-07-20: the operator configures an existing S3 output root and owns lifecycle policy; CDF creates a unique attempt namespace and owns only materialization identity and generation-safe consumption. CDF does not inspect/manage lifecycle, require delete permission, or generalize the destination staging lease.
- Blocked for external execution: Athena necessarily incurs query billing and writes result objects even when source tables are read-only. The exact workgroup, output root/prefix, billed scan ceiling, and query/result-object write authority require explicit confirmation before the live spike writes anything. Lifecycle remains operator-owned rather than a CDF side effect.

## Journal

- 2026-07-19: Opened as a research/spike owner rather than prematurely treating Athena as a Trino alias. The strategic distinction is accepted: direct Iceberg provides snapshot-native zero-service planning; Athena provides managed distributed SQL planning/pushdown and a columnar Parquet handoff that should reuse CDF's existing fast path.
- 2026-07-19: Activated after I2 reached the measured FQ12 remote-transfer roofline. Current AWS protocol and CDF boundary findings are recorded in `.10x/research/2026-07-19-athena-unload-source-protocol.md`. The retained architecture fixes schema with a bounded zero-row Athena query, records a final materializer program in the plan, executes `UNLOAD` as a runtime control task, freezes its paginated service manifest into canonical external Parquet tasks, and sends those tasks through the existing scheduler/data plane. `resolve()` side effects, a giant adapter-owned partition, Athena branches in generic execution, and query-result reuse are rejected.
- 2026-07-19: Read-only FQ12 inspection found engine-v3 workgroups and one historical successful `UNLOAD`. Its API evidence reported 305,031 rows, 3,216,216 scanned bytes, 2,597 ms total, and one manifest URI; the referenced output object was already absent. No SQL, result location, credential, query submission, or S3 mutation was performed. The observation permanently requires replay-time object-generation validation rather than trusting retained Athena query history.
- 2026-07-20: Re-entered shaping after the Iceberg/Glue program closed. Current source/runtime inspection confirms the source-neutral runtime partition-materialization seam does not yet exist: adapters may return static external task sets from `negotiate`, but no run-start control task can freeze provider-created partitions before scheduler execution. The existing fenced `StagingLease` implementation is semantically destination/load-specific (`DestinationId`, `TargetName`, `LoadAttemptId`) and must be generalized rather than reused under false names for Athena-owned output. No Athena implementation file was opened while these lifecycle, cleanup, disposition, and cost semantics remain unratified.
- 2026-07-20: All product semantics are now ratified. Follow-up research rejected the prior staging-lease generalization: lifecycle is operator-owned and CDF has no Athena-output collector to fence. The retained generic currency is an immutable materialized-query result and external task set beneath a user-configured output root. Cross-provider evidence also fixes a capability-driven doctrine: native direct read for BigQuery/Snowflake, provider spooling for Trino, `UNLOAD` for Athena, and measurement before choosing Redshift's transport. Findings and sources are preserved in `.10x/research/2026-07-19-athena-unload-source-protocol.md`.

## Blockers

Product shaping is unblocked. Live execution still requires explicit authorization for the exact FQ12 workgroup, operator-owned output root, billed scan ceiling, and deletion/retention side effects; the presence of an existing bucket does not itself authorize query billing or object writes.

## Evidence

Pending execution.

## Review

Pending.

## Retrospective

Pending.
