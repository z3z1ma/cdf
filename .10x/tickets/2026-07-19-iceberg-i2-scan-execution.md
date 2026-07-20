Status: active
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

- 2026-07-19: Activated after I1/F4 closure. Execution will consume the existing external canonical task artifact and injected neutral object-access authority; no upstream Iceberg runtime, private pool, or generic source branch will be introduced. The first tranche is one immutable Parquet task to a preaccounted ordinary `ResourceStream`, then evolution/defaults/partition constants and delete semantics extend that single path.
- 2026-07-19: Commit `855be499` installed a source-neutral external partition reader and cardinality-independent schedule/frontier authority. Iceberg now decodes one canonical task at a time while retaining its task-store bytes and parse lease; generic orchestration sees only `ExecutablePartition` plus opaque adapter retention.
- 2026-07-19: Implemented the first complete scan path through the aligned Iceberg Arrow 58 reader. Per-task blocking preparation resolves exact object generations through injected CDF object access, then a CPU stream performs field-ID projection, schema evolution/default filling, and Parquet decode. Ranged `AccountedBytes` cross the foreign reader zero-copy through `Bytes::from_owner`, retaining their CDF lease until the final foreign slice drops. Output batches reserve/reconcile the shared ledger and carry exact table-snapshot positions. Batch rows, retained bytes, metadata prefetch, range coalescing/concurrency, and internal stream depth are explicit source knobs.
- 2026-07-19: Added exact v2 position/equality-delete planning without adopting upstream's private, table-resident delete index. Delete manifests are loaded once in canonical bounded worker order into an invocation-local SQLite B-tree under a task-store-owned workspace. The cache plus maximum task assembly are memory-leased; database and rollback-journal growth is bounded by the injected shared spill reservation, whose page ceiling grows in an operator-configured quantum until the shared disk authority refuses it. Data entries query global equality and exact `(partition spec, typed partition tuple)` delete applicability with Iceberg sequence and referenced-file rules, then serialize canonical descriptors into the ordinary portable task. No SQLite path or implementation detail enters plan/task identity.
- 2026-07-19: Extended the real local v2 fixture across schema and partition evolution: the old file and its referenced position delete use unpartitioned spec 0, while the current file and a partition-scoped equality delete use bucket spec 1; a second unpartitioned equality delete remains global. Canonical task construction and the official aligned reader prove the semantics end to end: each delete reaches only its legal files by sequence/spec/typed tuple/reference, five of eight rows survive, schema evolution/default filling remains correct, and jobs 1/16 produce identical scan/task identities.
- 2026-07-19: The execution-boundary audit found and removed a latent scheduler mismatch: Iceberg task partitions declared snapshot-safe reopen semantics while the source advertised unit-only retry granularity. External task records become ordinary scheduled partitions at the generic boundary, so the source now correctly advertises partition retry. The real external-task fixture compiles its `CompiledSourceExecutionPlan`, materializes each bounded task binding, and proves the generic schedule accepts snapshot reattestation with partition-granularity retry.

## Blockers

None.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg --lib`: 25 passed. The real local v2 fixture plans two files written under schema IDs 0/1, executes both canonical tasks, projects the current two-column schema, fills the evolved optional field for the old file, emits eight rows, and attests the fixed snapshot.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-memory --lib`: 22 passed, including pointer-identity and lease-lifetime proof for zero-copy foreign `Bytes` ownership.
- `CARGO_BUILD_JOBS=12 cargo check -p cdf-engine -p cdf-project`: passed, proving the external-task and prepared-CPU seams compose through generic engine/project consumers.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg -p cdf-task-store --lib --locked -j 12`: 26 Iceberg tests and 4 task-store tests passed; the slow million-task task-store law remains explicitly ignored in the fast tier. The local Iceberg fixture physically applies both delete forms across schema/spec evolution and proves five exact surviving rows plus jobs-invariant task/scan hashes.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-iceberg -p cdf-task-store --all-targets --no-deps --locked -j 12 -- -D warnings`: passed. A dependency-inclusive invocation separately found an unrelated `needless_borrow` in the concurrently modified `cdf-runtime/src/watermark.rs`; I2 did not edit that worker-owned file.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg nonempty_snapshot_plans_canonical_tasks_independent_of_source_jobs --lib --locked -j 12`: passed. Beyond reader/delete results, the external task now traverses `CanonicalPartitionSchedule::scheduled_partition` and proves its compiled retry authority is `Partition`, preventing the prior unit/partition mismatch from reaching a real run.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-iceberg --all-targets --no-deps --locked -j 12 -- -D warnings`: passed after the retry-boundary repair.
- Limits: manifest-list cardinality is still resident, engine preview parity and completion/schema evidence still need a bounded audit, retry/cancellation fault injection is incomplete, and performance/REST/live Glue evidence remain open in I2.

## Review

Pending.

## Retrospective

Pending.
