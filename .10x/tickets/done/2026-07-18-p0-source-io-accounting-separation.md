Status: done
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-18-p0-post-iceberg-integration-stabilization.md
Depends-On: .10x/tickets/done/2026-07-18-p0-external-partition-authority.md

# P0: separate planned work from observed source I/O

## Scope

Make planned task/file byte estimates a typed compiler fact while retaining `SourceIoMetrics` as the sole authority for actual physical bytes transferred. Repair benchmarks and reports without reopening all external tasks merely to reconstruct a counter.

## Acceptance Criteria

- Planned estimates and observed physical/useful/logical bytes have distinct types and labels.
- Runtime metrics are recorded at source I/O boundaries only.
- Benchmarks consume actual physical bytes for throughput and may separately report planned bytes.
- No task-set re-enumeration exists solely for runtime byte accounting.

## Assumptions

- User-ratified: represented work bytes and transferred bytes are not interchangeable.

## Journal

- 2026-07-18: Activated after the independent audit proved the benchmark reopened every external task and mislabeled represented object sizes as physical transfer bytes. Introduced typed `PlannedSourceBytes` on `ScanPlan`; file inventory computes it during its one authoritative traversal and retains it beside the external task-set reference.
- 2026-07-18: Deleted benchmark task-set re-enumeration. File and Iceberg runners now report source-phase physical bytes from runtime `SourceIoMetrics` and retain planned bytes as a separately labeled fact. Partial/cancelled partition termination now snapshots source-owned I/O counters after the producer joins, so the engine does not lose terminal evidence merely because EOF was not reached.
- 2026-07-18: Repaired the benchmark lab's isolated-run timeout test so it tests sample isolation rather than depending on a five-second whole-workspace contention assumption. The complete workspace barrier then passed 1,771/1,771, including the benchmark suite, and strict all-target Clippy passed.

## Blockers

None. The partition-authority dependency is closed.

## Evidence

- `cdf-source-files` production Parquet projection/predicate test passed and additionally proves partial stream termination retains nonzero request and physical-byte metrics.
- `cdf-benchmarks::lab_runners prepared_multi_file_jobs_matrix_preserves_canonical_package_identity` passed with planned bytes sourced from the compiled scan and actual physical bytes sourced from engine phase telemetry.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo nextest run --workspace --locked -j 12 --no-fail-fast` ran 1,771 tests: 1,771 passed, 40 explicitly skipped.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo clippy --workspace --all-targets --locked -j 12 -- -D warnings` passed.

## Review

Verdict: pass. A fresh-hat sequential review traced `PlannedSourceBytes` from source-owned planning and `SourceIoMetrics` from observed byte-source counters through full and partial completion into engine phase telemetry and benchmark throughput. Planned bytes are never relabeled as transferred bytes, and no benchmark reopens an external task set merely to count represented work.

Residual risk: metrics callbacks intentionally fail soft after a poisoned observation mutex because they are non-identity operational telemetry; preparation itself still fails closed if that mutex is poisoned while installing the observer. The collaboration thread limit prevented commissioning a new independent agent without reusing an old reviewer.

## Retrospective

The migration exposed a common measurement error: object sizes represented by a plan are not bytes physically transferred. Actual I/O must originate at the byte-source boundary, including partial termination. Estimates remain useful, but only as explicitly typed plan facts.
