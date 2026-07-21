Status: done
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

- 2026-07-18: Activated after the first six stabilization children closed. The audit finding is
  confirmed in source: canonical partition/unit ordinals and retry/worker evidence were `u32`,
  and execution rejected external task counts above `u32::MAX` before opening bounded work.
- 2026-07-18: Migrated portable partition and decode-unit ordinals to `u64` through scheduler,
  source frontier, retry journal, portable worker tasks, engine partition evidence, segmentation,
  drain/replay positions, Parquet row groups, Avro blocks, and benchmark/source fixtures. Kept
  host-local concurrency/capacity and inline-vector indexing as `usize`, with checked conversion
  only when an inline resident vector is actually indexed.
- 2026-07-18: `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo check --workspace --all-targets
  --locked -j 12` passed. This proves the workspace type migration compiles; it does not yet prove
  high-ordinal artifact round trips or product behavior.
- 2026-07-18: A fresh-hat boundary audit found scheduler resolution and staged drain execution
  still narrowing the total task count to `usize` merely to compute a bounded job ceiling. Changed
  scheduler cardinality APIs to `u64`; only inline-vector lookup and the already-admitted host job
  count now cross into `usize`.
- 2026-07-18: Added direct regression tests for compiling and scheduling an external task at
  `u32::MAX + 17`, retry-evidence JSON at `u32::MAX + 23`, and portable worker-task JSON at
  `u32::MAX + 29`.

## Blockers

None.

## Evidence

- External count/ordinal: `cdf-engine::tests::external_partition_schedule_preserves_ordinals_above_u32_without_enumeration`
  compiled an external authority with 4,294,967,313 tasks and scheduled its final task without
  enumerating the task set.
- Retry artifact: `cdf-runtime::retry::tests::retry_evidence_round_trips_external_partition_ordinal_above_u32`
  serialized, deserialized, and revalidated high-ordinal evidence against external schedule
  authority.
- Worker artifact: `cdf-runtime::worker_protocol::tests::portable_partition_task_round_trips_partition_ordinal_above_u32`
  serialized and deserialized a digest-bound portable task above the old ceiling.
- Static migration: `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo check --workspace
  --all-targets --locked -j 12` passed after scheduler callers, format drivers, source adapters,
  engine, project runtime, CLI, Python tests, and benchmarks moved to portable `u64` cardinality.
- Lint gate: `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo clippy --workspace --all-targets
  --locked -j 12 -- -D warnings` passed.
- Integration gate: `NEXTEST_HIDE_PROGRESS_BAR=1 CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo
  nextest run --workspace --locked -j 12 --status-level pass --final-status-level pass` passed
  1,774/1,774 tests with 40 explicit skips in 465.063 seconds. This includes engine jobs
  invariance, recorded multi-file jobs invariance, Iceberg external task authority, file external
  planning, retry/replay, and all repeated package/live-run goldens.

## Review

Verdict: pass.

Fresh-hat review searched every production partition/task/count conversion after the migration.
No canonical partition or decode-unit ordinal remains `u32`; remaining `u32` ordinals belong to
per-partition segments, destination batches/objects, or statistics fields outside this ticket.
The only portable-ordinal `usize` conversions are checked inline resident-vector lookups and the
bounded active-job result. No external task-set count is materialized, enumerated, or rejected by
process address space. No performance-sensitive payload operation changed; the scheduler now does
less cardinality conversion, while small existing JSON artifacts retain identical numeric bytes.

Residual risk: the public post-construction partition-authority setter and representation-sensitive
source SDK remain dangerous, but are explicitly owned by
`.10x/tickets/2026-07-18-p0-source-planning-authority-closure.md` and are not needed to preserve the
portable `u64` protocol.

## Retrospective

The first migration compiled but was not complete: searching only stored field types missed the
total-count narrowing inside scheduler resolution. The reliable audit is semantic: trace one
high ordinal from external reference through admission, worker serialization, retry evidence,
segment assembly, and drain/replay, then separately search every `usize::try_from` and every
`u32::try_from`. Preserve this distinction in future scale work: portable cardinality is `u64`;
resident collection indices and active concurrency are host-local bounded types.
