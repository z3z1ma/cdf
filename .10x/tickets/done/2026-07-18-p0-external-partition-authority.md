Status: done
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-18-p0-post-iceberg-integration-stabilization.md

# P0: close and migrate partition authority

## Scope

Replace `ScanPlan`'s independently representable inline partition vector and optional external task set with one closed `PartitionAuthority` sum type. Migrate planning, preview, execution, replay, deep validation, conformance, benchmarks, and tests to consume the authority exhaustively or through kernel-owned methods.

## Non-goals

- No source-specific branch in generic orchestration.
- No inline fallback for external task sets.
- No retention of the invalid two-field serialized form; CDF has no compatibility obligation.
- No change to canonical task ordering or payload execution semantics.

## Acceptance Criteria

- Invalid inline-plus-external and empty-implicit authority combinations are unrepresentable.
- Production callers do not inspect an inline vector when the scan authority may be external.
- Preview, deep validation, execution, incrementality, replay evidence, and benchmark accounting consume canonical authority without payload contact beyond their explicit phase.
- Direct-partition regression tests are migrated to the public authority contract, not weakened.
- Focused integration tests, formatting, strict Clippy, and the partition-authority slice of the stabilization suite pass. The parent barrier remains the authority for unrelated whole-suite failures.

## References

- `.10x/tickets/done/2026-07-19-iceberg-f4-externalized-scan-tasks.md`
- `.10x/tickets/done/2026-07-11-p0-wx1-portable-partition-task-protocol.md`
- `.10x/tickets/2026-07-18-p0-post-iceberg-integration-stabilization.md`

## Assumptions

- User-ratified: the two-field authority representation is an architectural defect and must be replaced by one closed sum type.
- Record-backed: external task records remain source-owned, content-addressed, ordered, and decoded through `PlannedPartitionReader`.

## Journal

- 2026-07-18: Activated after the integration barrier found preview/run parity, project tests, deep validation, conformance, and benchmark callers still reading `scan.partitions` after file scans moved to external authority. Deep validation was immediately repaired to negotiate and stream the task reader in constant metadata memory; the closed kernel representation remains the compile-enforced completion mechanism.
- 2026-07-18: Replaced the two-field representation with private `ScanPlan::partition_authority: PartitionAuthority`, required construction through `ScanPlan::new`, and exhaustive inline/external accessors. Migrated every production and test caller across kernel, runtime, engine, project, CLI, sources, destinations, conformance, and benchmarks. `rg` found no remaining direct `ScanPlan.partitions` or `planned_task_set` access; similarly named occurrences are unrelated report/watermark data.
- 2026-07-18: External task execution now registers streamed partition ids with watermark authority without pre-materializing the task set. Complete external drain epochs rebind through the source-owned resume seam; partial external task slicing remains explicitly rejected and is owned by `.10x/tickets/2026-07-20-source-resume-aware-negotiation.md`.
- 2026-07-18: Generic DataFusion table-provider execution now rejects external task authority at planning rather than silently producing a zero-partition plan. The source-owned DataFusion provider/plan-shell bridge remains owned by `.10x/tickets/2026-07-12-p3-j5-execution-plan-marshaling-metrics.md`.
- 2026-07-18: The full `cdf-engine` crate ran 191 tests: 184 passed and seven failed for independently existing stabilization defects (benchmark runtime-ownership static policy, invalid manifest-hash fixtures, memory accounting, stale widening expectations, and package rechunk identity). None exercised the partition-authority migration. These remain visible under the parent P0 barrier and are not claimed as green here.

## Blockers

None.

## Evidence

- Closed representation: `scan_plan_serialization_has_one_partition_authority_and_rejects_the_retired_shape` proves serialized plans carry exactly one tagged authority and the retired two-field shape no longer deserializes. `external_task_authority_is_a_closed_alternative_to_inline_partitions` proves external cardinality and access semantics. `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo nextest run -p cdf-kernel --locked -j 12` passed 73/73.
- Authority owners: `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo nextest run -p cdf-kernel -p cdf-runtime -p cdf-source-files -p cdf-source-glue -p cdf-source-iceberg --locked -j 12 --no-fail-fast` passed 325/325 with two skipped.
- Cross-layer behavior: the selected CLI/project/conformance run covering all six `validate_deep_` cases, declared multi-file Parquet, both HTTP template planners, drain settlement, S8 multi-file preview/run parity, and generic external-source run/replay passed 11/11.
- Static quality: `cargo fmt --all`; strict all-target Clippy across kernel, runtime, engine, files, Glue, Iceberg, project, CLI, and conformance passed with `-D warnings`. Earlier whole-workspace `cargo check --workspace --all-targets --locked -j 12` and strict whole-workspace Clippy also passed for the integrated worktree.
- Production migration audit: `rg -n "\\.partitions|planned_task_set" crates --glob '*.rs'` found no retired `ScanPlan` representation use; remaining hits are unrelated structures or the new external-authority vocabulary.

## Review

Fresh-hat self-review was required because the collaboration thread had no unused reviewer slot and the user prohibited reusing old agents. The review traced construction, serialization, schedule compilation, source readers, preview, deep validation, incrementality, replay, drain/watermark state, CLI rendering, conformance, and DataFusion query adaptation. No critical or significant defect remained. The two incomplete consumers are explicit failures with existing owners rather than hidden materialization fallbacks: partial external drain slicing and source-owned DataFusion table providers. Verdict: pass. Residual risk is confined to those named tickets and the parent stabilization failures recorded above.

## Retrospective

The regression multiplier was representational: an empty inline vector plus an optional external reference let callers compile while reading the wrong authority. Turning the state into a closed enum converted silent omissions into compile errors across the whole graph. For high-cardinality migrations, cardinality, iteration, reporting, and resume must remain separate capabilities; a count must not imply resident task access, and a streamed task set must not be reopened merely to satisfy UI or benchmark counters.
