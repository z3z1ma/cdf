Status: done
Created: 2026-07-19
Updated: 2026-07-20
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md
Depends-On: .10x/tickets/done/2026-07-19-iceberg-i2-scan-execution.md

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
- 2026-07-19: Activated after I2 closed with bounded execution at the measured remote-transfer roofline. I3 retains the already-landed source-neutral unchanged-position no-op and now owns the remaining append-ancestry, time-travel, replay, unsupported-capability, and final FQ12/local conformance acceptance criteria.
- 2026-07-20: Added explicit `snapshot` and `append_snapshots` resource modes. Canonical scan tasks now record the manifest's positive `added_snapshot_id`; append resume proves catalog/table/ref identity, the committed snapshot authority, exact parent ancestry, and `append` operation for every intervening snapshot before streaming the content-addressed task set into a canonical delta with contiguous ordinals and recomputed estimates. Missing history, divergent history, and overwrite/delete/replace operations fail with both specified remedies before payload or destination mutation.
- 2026-07-20: Added local conformance for a current snapshot containing old and newly appended manifests, exact one-file delta selection, explicit historical snapshot selection after the catalog current pointer advanced, missing/divergent ancestry, and overwrite rejection. Driver/task authority versions advanced with no legacy decoder because CDF has no compatibility obligation. Added the operator capability matrix in `docs/iceberg.md`.
- 2026-07-20: The existing generic lifecycle still performs full task planning before post-plan resume binding. Correctness and payload selection are exact, but unchanged and append runs retain avoidable metadata/control-plane work. The source-neutral, pre-planning correction is owned by `.10x/tickets/2026-07-20-source-resume-aware-negotiation.md`; no Iceberg state access or generic source-kind branch was admitted here.

## Blockers

None. I3 required no AWS mutation; existing authorized read-only FQ12 evidence remains applicable.

## Evidence

- Exact unchanged-snapshot no-op: `cargo test -p cdf-project bounded_table_snapshot_run_rebinds_unchanged_frontier_to_no_op --lib --locked -j 12` passed; proves no second source open or package/destination/checkpoint mutation.
- File incrementality regression: `cargo test -p cdf-project file_manifest_append_run_skips_unchanged_files_and_loads_only_changes --lib --locked -j 12` passed after the generic orchestration change.
- Iceberg task authority regression: `cargo test -p cdf-source-iceberg nonempty_snapshot_plans_canonical_tasks_independent_of_source_jobs --lib --locked -j 12` passed and now asserts exact-snapshot rebinding clears both partitions and the external task set.
- Static checks: `cargo fmt --all -- --check` and strict `cargo clippy -p cdf-engine -p cdf-project -p cdf-source-iceberg -p cdf-cli --all-targets --no-deps --locked -j 12 -- -D warnings` passed.
- Authorized live FQ12 read-only run: `cdf --json run lake.dim_date` returned `reason=source_position_unchanged`, `writes={package:false,destination:false,checkpoint:false}`, and three lifecycle events. A DuckDB query after the run returned 2,194 rows and 2,194 distinct `_cdf_row_key` values. Limit: the existing 2x duplicate remains from the pre-fix reproducer; this run proves no third copy was added.
- Append/time-travel conformance: `cargo test -p cdf-source-iceberg append_snapshot_resume_selects_only_new_files_and_rejects_nonappend_history --lib --locked` passed. It proves two current-snapshot tasks reduce to the one five-row/one-byte manifest added by snapshot 101, rewritten ordinal zero; snapshot 100 remains selectable after current advanced to 101; missing/divergent ancestry and overwrite fail before task admission with exact remedies.
- Crate conformance: `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg --lib --locked` passed all 39 tests, including v1/v2, local filesystem/REST/Glue binding, deletes, retry/cancellation, generation mutation, jobs invariance, task spill/million-manifest residency, preview/run, and unchanged-snapshot laws.
- Static analysis: `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-iceberg --all-targets --no-deps --locked -- -D warnings` passed.

## Review

Fresh-hat adversarial review, 2026-07-20. Findings: no critical or significant correctness, security, determinism, memory-boundedness, or extension-boundary defects. The delta task set preserves the selected snapshot authority and validates every source record before filtering; ordinal and estimate rebinding are deterministic; failure precedes source payload/destination mutation; the default snapshot data path does not perform incremental filtering. Minor residual: resume binding occurs after full task planning, so the no-op is payload-fast but not yet control-plane-fast. Verdict: pass with the separately owned source-neutral lifecycle performance ticket. Residual risk: malformed catalog history is limited to the metadata authority available at planning; expired history fails closed as designed.

## Retrospective

The existing typed `TableSnapshotPosition` and external task store were sufficient; no new kernel snapshot type or Iceberg-specific orchestration was needed. The essential distinction is between snapshot ancestry authority and file-sequence authority: v1 sequence numbers cannot identify append provenance, so the manifest's `added_snapshot_id` must ride in each portable task. Post-plan rebinding was the smallest correct implementation but exposed a general lifecycle ordering issue; recording that separately prevents a performance workaround from contaminating the source boundary.
