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
- 2026-07-19: Installed the matching executable-partition reattestation hook. A retry revalidates the retained task against its content-addressed task-set authority and returns the exact pinned table snapshot plus compiled Arrow schema hash; it neither resolves catalog `current` nor opens payload. This closes the prior failure mode where partition retry compiled successfully but the generic retry loop received `None` from the source before reopen.
- 2026-07-19: Closed the external-task preview/run parity gap generically. Preview previously enumerated only inline `ScanPlan::partitions`, so an Iceberg plan truthfully reported two external tasks but preview inspected zero. The kernel now exposes a cardinality-bounded ordered form of the existing stratified-hash selector; the ordinary selector delegates to the same implementation, preserving v1 membership exactly. Engine preview makes a metadata-only counting pass over external tasks, then retains at most the operator's `max_batches` selected executable task payloads on a second pass, opens them through `open_executable`, and uses executable reattestation. No source identity branch entered the engine. The real evolved/deleted Iceberg fixture now compiles through `Planner::plan_tier_b`, previews five rows from two tasks, executes the same plan into a verified package, and records table-snapshot segment positions and checkpoint-eligible completion evidence.
- 2026-07-19: Proved the retry lifecycle against real Iceberg I/O rather than only schedule shape. A test-only neutral `FileTransport` wrapper injects one transient metadata failure into canonical data task zero after discovery and preview. Generic engine execution reattests the retained external task, reopens only that partition, completes the same five-row package, and records one transient retry history entry for ordinal zero. The source owns no retry loop and never re-resolves catalog `current`. The test execution host's delay implementation was also corrected to schedule timers on its owned Tokio runtime while remaining safe to poll from the executor-agnostic engine test, matching the `ExecutionHost` contract.
- 2026-07-19: Closed retry generation drift without a run-wide identity map. Each ledger-owned retained task now carries one fixed 256-byte attempt-attestation slot shared by its bounded retry clones. A complete prepare hashes the exact transport identities of that task's data/delete objects in canonical path order. Later attempts must match before decode begins; range reads still enforce the same identity within an attempt. The real fixture prepares task zero, replaces its Parquet object with a same-sized generation, and proves the next prepare fails as `Data`. This state is invocation-only and never enters task/package identity.
- 2026-07-19: Added cooperative cancellation fault injection at the neutral transport boundary. Cancellation during canonical data-task metadata preparation terminates generic engine execution, consumes the configured signal, and leaves the package lifecycle at `extracting`; it never reaches validation, packaging, or checkpoint eligibility. No Iceberg-specific cancellation loop or package behavior was introduced.
- 2026-07-19: Removed resident manifest-list cardinality from scan planning. A minimal v1/v2 Avro decoder now streams each manifest-list entry into the same invocation-local, spill-backed planning index that owns delete applicability; the upstream whole-list `Vec` and separate delete-only index are gone. Manifest identities remain canonically ordered through a `(content, path)` B-tree, counted in constant control memory, and fed to bounded parallel manifest workers without collecting the snapshot. The planning-index cache, reader cache, maximum task assembly, database, and rollback journal remain subordinate to the injected memory/spill authorities through explicit source knobs.
- 2026-07-19: Adversarial review rejected a naive statement-level `SQLITE_FULL` retry inside one transaction: SQLite may roll back the entire transaction, making that retry silently incomplete. The retained implementation treats a full insert/commit as a failed attempt, verifies that rollback retained zero manifest rows, grows only its shared spill reservation, and deterministically reparses the already-accounted manifest-list payload. A 10,000-manifest test forces 64 KiB growth quanta, proves canonical traversal and fixed ledger residency, and completes in 1.03 seconds in the debug test profile. Initial worker assignment was also moved inside scope cleanup so iterator/queue failures cannot strand blocking workers.
- 2026-07-19: Extended REST beyond empty discovery. A negotiated nonempty REST response now drives the real evolved v2 fixture through manifest/delete planning, two canonical external tasks, neutral local object access, aligned Parquet decode, and both delete forms to the exact five-row result. Discovery and resolution still make only the two catalog protocol requests (config and load-table); payload execution never recontacts the catalog or takes a REST-specific execution path.
- 2026-07-19: Closed the v1 execution gap with a real v1 table rather than treating manifest-list decoding as scan proof. The fixture writes v1 metadata, a v1 data manifest, v1 manifest list, field-ID-bearing Snappy Parquet, and one fixed snapshot. Ordinary filesystem discovery freezes its Arrow schema; planning externalizes one canonical task whose shared authority records table format v1; aligned execution emits the exact three rows.
- 2026-07-19: Proved projection and predicate fidelity through the complete engine boundary. Iceberg truthfully classifies `id > 4` as unsupported rather than claiming source pruning; the engine retains `id` only long enough to evaluate the residual after delete application, then packages only projected `label` plus the governed residual/provenance columns. The evolved/delete fixture yields exactly `[null, "eight"]`, demonstrating that deleted IDs 5/7 stay deleted, residual filtering removes the old/current nonmatching rows, and an internal filter dependency does not leak into the projected data schema.

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
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg nonempty_snapshot_plans_canonical_tasks_independent_of_source_jobs --lib --locked -j 12`: passed with both tasks reattested through `ResourceStream::attest_executable`; each yields its immutable table-snapshot position and the exact compiled schema hash before ordinary execution.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-iceberg --all-targets --no-deps --locked -j 12 -- -D warnings`: passed after installing executable reattestation.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg nonempty_snapshot_plans_canonical_tasks_independent_of_source_jobs --lib --locked -j 12`: passed. The real fixture now additionally proves generic engine preview/run parity: two planned/eligible/opened external partitions, five preview rows, five packaged rows, two lineage partitions, only table-snapshot segment positions, and checkpoint-eligible execution evidence.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel stratified_selection --lib --locked -j 12`: 4 passed. Ten thousand ordered candidates retain at most 64 task identities throughout selection and produce the exact ordinary v1 plan; permutation, edge, and single-member laws remain green.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine preview_large_plan_selects_and_opens_at_most_the_global_batch_budget --lib --locked -j 12` and `... preview_traverses_every_planned_partition_through_the_engine_front_end ...`: passed. The existing inline bounded-selection and full traversal behavior remains unchanged. A broader `preview_` filter had seven passes and one pre-existing planning failure in `preview_terminal_quarantine_uses_run_attestation_without_opening_payloads` (`effective schema discovery manifest does not match its pinned schema snapshot`) before preview execution; that unrelated fixture/authority drift is outside I2.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-engine -p cdf-source-iceberg --all-targets --no-deps --locked -j 12 -- -D warnings`: passed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg nonempty_snapshot_plans_canonical_tasks_independent_of_source_jobs --lib --locked -j 12`: passed after one injected transient failure. The run consumed the failure, emitted five rows across the same two snapshot-positioned partitions, remained checkpoint-eligible, and exposed exactly one retry evidence record for partition ordinal zero with a selected delay and no exhaustion.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg --lib --locked -j 12`: 26 passed after the transport fault-injection tranche.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-iceberg --all-targets --no-deps --locked -j 12 -- -D warnings`: passed after the transport fault-injection tranche.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg nonempty_snapshot_plans_canonical_tasks_independent_of_source_jobs --lib --locked -j 12`: passed with cancellation during task preparation and same-sized object replacement between prepared attempts. The cancelled package remained exactly `extracting`; the replaced generation failed before decode.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg --lib --locked -j 12`: 26 passed after cancellation and generation attestation.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-iceberg --all-targets --no-deps --locked -j 12 -- -D warnings`: passed after cancellation and generation attestation.
- `/usr/bin/time -p env CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg manifest_cardinality_does_not_change_ledger_residency --lib --locked -j 12`: passed in 1.03 seconds of test execution. Ten thousand reverse-ordered manifests forced repeated 64 KiB spill-reservation growth and deterministic transaction replay, then traversed in canonical order while CDF ledger residency stayed exactly fixed; dropping the index returned both memory and spill usage to zero.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg --lib --locked -j 12`: 28 passed. This includes real v1 manifest-list decoding, the evolved v2 data/delete execution fixture, canonical external tasks, retry/cancellation/generation fault injection, spill-backed delete applicability, and the new high-cardinality manifest law.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-iceberg --all-targets --no-deps --locked -j 12 -- -D warnings`: passed after the spill-backed manifest planner and transaction-replay correction.
- `git diff --check -- Cargo.lock crates/cdf-source-iceberg`: passed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg rest_catalog_nonempty_snapshot_executes_canonical_local_objects --lib --locked -j 12`: passed. The REST binding negotiated embedded metadata exactly once, planned two canonical tasks from local manifest objects, read local Parquet/delete objects through the neutral transport, and produced five surviving rows without a third catalog request.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg --lib --locked -j 12`: 29 passed after nonempty REST execution coverage.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-iceberg --all-targets --no-deps --locked -j 12 -- -D warnings`: passed after the REST execution tranche.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg v1_parquet_snapshot_discovers_plans_and_executes --lib --locked -j 12`: passed. The generated v1 metadata/manifest-list/manifest/Parquet table discovered one field, planned one external task with `table_format_version=1`, and executed three rows.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg --lib --locked -j 12`: 30 passed after adding real v1 execution.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-iceberg --all-targets --no-deps --locked -j 12 -- -D warnings`: passed after the v1 execution tranche.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg nonempty_snapshot_plans_canonical_tasks_independent_of_source_jobs --lib --locked -j 12`: passed with a projected `label` package and unsupported `id > 4` residual. Two rows survive with values `[null, "eight"]`; package columns are exactly `label`, `_cdf_variant`, and `_cdf_package_row_ord`.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg --lib --locked -j 12`: 30 passed after the projection/residual integration law.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-iceberg --all-targets --no-deps --locked -j 12 -- -D warnings`: passed after the projection/residual tranche.
- Limits: socket-level local REST/remote-object and roofline measurements plus live Glue evidence remain open in I2.

## Review

Pending.

## Retrospective

Pending.
