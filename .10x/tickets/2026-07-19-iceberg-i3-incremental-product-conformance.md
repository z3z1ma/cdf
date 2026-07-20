Status: blocked
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md
Depends-On: .10x/tickets/2026-07-19-iceberg-i2-scan-execution.md

# Iceberg I3: snapshot incrementality, product parity, and full conformance

## Scope

Implement fixed-snapshot/time-travel and append-only snapshot ancestry/no-op semantics; complete preview/run/replay/product diagnostics; close local REST/filesystem and authorized FQ12 Glue/S3 performance/conformance for the ratified v1/v2 Parquet matrix.

## Non-goals

No changelog/tailing approximation, catalog writes, ORC/Avro/v3/encryption silent support, or persistent AWS fixture after testing.

## Acceptance Criteria

- Append ancestry selects only appended files and rejects rewrite/delete/divergent/missing history with exact remedies.
- Preview/run/replay after catalog advancement preserve the pinned snapshot and package identity.
- All source-extension conformance laws and local Iceberg matrix pass.
- Authorized FQ12 Glue/S3 run meets P3 network/Parquet overhead targets with resource setup/teardown and reproducible evidence.
- Unsupported capability matrix is explicit in plan/doctor/docs; every follow-up has a durable owner.

## References

- `.10x/specs/iceberg-source.md`
- `.10x/specs/source-extension-runtime-contract.md`

## Assumptions

- User-ratified 2026-07-19: local and FQ12 live testing are required; external provisioning is confirmed separately when concrete resources are known.

## Journal

- 2026-07-19: I2's authorized FQ12 smoke run exposed the first concrete I3 acceptance failure. Two bounded runs selected the exact same Glue Iceberg snapshot (`snapshot_id=2229073605200099107`, sequence 7, identical metadata generation) and each appended 1,097 rows to DuckDB. The checkpoint head correctly records identical input/output `TableSnapshotPosition` values, proving the missing behavior is generic bounded-source resume/no-op binding before package creation rather than catalog drift. I3 MUST replace this duplicate execution with the specified visible fast no-op and a permanent local two-run regression scenario before closure.
- 2026-07-19: Implemented the source-neutral unchanged-position path through the existing `ResourceStream::rebind_scan_for_resume` seam. Bounded orchestration now loads the committed frontier before package creation, the Iceberg adapter removes task authority only when the selected `TableSnapshotPosition` is byte-for-byte identical, and the engine recompiles an empty deterministic schedule. No generic runtime branch names Iceberg. A permanent two-run project test proves that the second run opens no partition, emits no package/destination/checkpoint write, creates no package directory, and returns `source_position_unchanged`; the existing FileManifest no-op law remains green.
- 2026-07-19: Live FQ12 verification against `gold.dim_date` selected the already-committed snapshot `2229073605200099107` and returned an explicit no-op with `planned_packages=0`, three lifecycle events, and all three write flags false. DuckDB remained at 2,194 rows/2,194 distinct row keys, so the prior duplicate-load regression is closed for an exact unchanged snapshot. The run still took 3.44 seconds because task planning currently precedes resume rebinding; telemetry also showed 68,157,455 peak bytes for the external task-set writer and 9,502,720 for the Iceberg planning index on only four tasks. That is now concrete I2 control-plane performance evidence, not accepted I3 overhead.

## Blockers

I2. AWS external writes require confirmation at execution time.

## Evidence

- Exact unchanged-snapshot no-op: `cargo test -p cdf-project bounded_table_snapshot_run_rebinds_unchanged_frontier_to_no_op --lib --locked -j 12` passed; proves no second source open or package/destination/checkpoint mutation.
- File incrementality regression: `cargo test -p cdf-project file_manifest_append_run_skips_unchanged_files_and_loads_only_changes --lib --locked -j 12` passed after the generic orchestration change.
- Iceberg task authority regression: `cargo test -p cdf-source-iceberg nonempty_snapshot_plans_canonical_tasks_independent_of_source_jobs --lib --locked -j 12` passed and now asserts exact-snapshot rebinding clears both partitions and the external task set.
- Static checks: `cargo fmt --all -- --check` and strict `cargo clippy -p cdf-engine -p cdf-project -p cdf-source-iceberg -p cdf-cli --all-targets --no-deps --locked -j 12 -- -D warnings` passed.
- Authorized live FQ12 read-only run: `cdf --json run lake.dim_date` returned `reason=source_position_unchanged`, `writes={package:false,destination:false,checkpoint:false}`, and three lifecycle events. A DuckDB query after the run returned 2,194 rows and 2,194 distinct `_cdf_row_key` values. Limit: the existing 2x duplicate remains from the pre-fix reproducer; this run proves no third copy was added.

## Review

Pending.

## Retrospective

Pending.
