Status: done
Created: 2026-07-21
Updated: 2026-07-21

# P0: Iceberg execution robustness barrier

## Scope

Restore ordinary Iceberg execution after two independently exposed product regressions. This parent
owns ordering and the final real-project smoke only; executable work lives in its children.

## Children

1. `.10x/tickets/done/2026-07-21-p0-iceberg-task-observation-authority.md`
2. `.10x/tickets/done/2026-07-21-p0-iceberg-byte-adaptive-parquet-batches.md`
3. `.10x/tickets/cancelled/2026-07-21-p0-duckdb-wide-ingest-memory.md` (cancelled into D17)
4. `.10x/tickets/done/2026-07-21-p0-segmentation-v3.md`

## Acceptance Criteria

- Every external Iceberg task carries one stable, unique, source-owned effective-schema observation
  identity and canonical partition binding.
- Wide Iceberg Parquet data executes under default configuration without requiring the operator to
  guess a row count or weaken the shared memory authority.
- The real `flolake.transactions` project succeeds to Parquet under defaults, and the fixes
  introduce no source identity branch in generic orchestration. DuckDB wide-ingest memory remains
  solely owned by `.10x/tickets/cancelled/2026-07-18-p3-d17-duckdb-wide-string-overlap.md`.

## References

- `.10x/specs/iceberg-source.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/specs/deterministic-parallel-scheduler.md`
- `.10x/tickets/done/2026-07-19-iceberg-i2-scan-execution.md`
- `.10x/tickets/done/2026-07-13-p0-sa1-compiled-stream-admission-plan.md`

## Assumptions

- User-ratified 2026-07-21: ordinary execution MUST work under defaults; operators must not tune
  `parquet_batch_rows` or `maximum_batch_bytes` merely to make a valid table executable.
- Record-backed: Iceberg owns task decoding, physical batch sizing, and observation metadata; the
  kernel/runtime admission and memory invariants remain source-neutral and fail closed.

## Journal

- 2026-07-21: Opened after the FQ12 `bronze.transactions` smoke exposed a 105,606,290-byte decoded
  batch against the default 33,554,432-byte frontier and a separate Iceberg external task with no
  effective-schema observation identity. D17 work is paused and excluded from these repairs.
- 2026-07-21: The repaired source completed all 3.5 million rows to Parquet in 61.18 seconds under
  955 MB peak RSS. The required fresh DuckDB smoke then exposed a third, previously masked P0:
  host-wide native threads exhausted DuckDB's 3.3 GiB usable buffer-manager envelope on the
  2,052-column package. Added the focused destination child rather than weakening the parent smoke.
- 2026-07-21: The exact optimized Parquet-destination smoke now completes under defaults. Its phase
  evidence exposed and removed two O(columns squared) validation/normalization lookups; the same
  3,513,266-row/2,052-column workload improved 62.08 to 50.89 seconds. The remaining 1,188 canonical
  segments are governed by the pre-existing measured 32 MiB logical policy, not the Iceberg reader.
  This sparse wide schema repeats about 1.5 MiB of IPC schema framing per segment and turns 42 MiB
  of source objects into a 2.73 GiB package, so a measured schema-aware/configurable segmentation
  follow-up is now higher priority than unrelated backlog work.
- 2026-07-21: User ratified immediate replacement of the narrow default. Segmentation v3 raises the
  canonical target/maximum to 256 MiB and exposes every row/byte segment and microbatch boundary to
  plan, explain, preview, run, and backfill. The policy stays generic, plan-recorded, and replay
  deterministic; no Iceberg or destination branch selects package identity.

## Blockers

None.

## Evidence

Child evidence is populated. The final optimized FQ12 run committed 3,513,266 rows through all 84
external tasks to Parquet under defaults in 36.94 seconds, with 231 canonical segments and 1.18 GiB
peak RSS. The integrated main checkout passed workspace compilation, strict clippy, and 733 focused
CLI/contract/engine/project/runtime/Iceberg tests (with only the explicitly ignored performance
tests). The serial conformance run passed 95/97 before two fixture defects were repaired; each
repaired REST and external-source scenario then passed independently.

## Review

Pass. The final pass traced the source-owned task authority through registry execution, separated
decoder working-set and emitted-batch bounds, rejected pressure-dependent post-decode copying, and
verified that segmentation remains source/destination neutral. The only measured cost is an
approximately 194 MiB higher peak RSS on the deliberately extreme wide fixture, still below 1.2
GiB and well inside the default 4 GiB ledger; every capacity boundary is tunable and recorded.

## Retrospective

Three failures shared one lesson: narrow fixtures concealed both missing lifecycle authority and
quadratic schema work, while a narrow-schema segment default multiplied IPC framing on wide data.
The durable repair is to test complete product lifecycles with wide schemas, keep source memory
claims explicit, and treat identity-bearing performance policy as required plan data rather than
destination or source heuristics.
