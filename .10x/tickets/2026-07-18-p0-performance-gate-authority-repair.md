Status: active
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-11-p0-wx1-portable-partition-task-protocol.md

# P0: restore current execution and evidence authority in the performance gate

## Scope

Repair the authoritative benchmark suite so its CDF engine/package and file/destination cases execute through the current WX1 compiled-source, external canonical partition-schedule, operator-graph, and descriptor-bound destination-evidence boundaries. Delete stale pre-WX1 benchmark paths and superseded destination observations; do not weaken product validation or add a fallback.

## Non-goals

- No product data-plane change.
- No timing claim from laptop Criterion samples.
- No compatibility path for plans that omit current compiler authority.

## Acceptance Criteria

- `cargo test -p cdf-benchmarks --tests --locked` exercises a CDF engine package case through current authority and passes.
- `CDF_BENCH_SUITE=smoke cargo bench -p cdf-benchmarks --bench baseline --locked` completes every smoke cell instead of aborting at the engine package case.
- The generated performance envelope joins current selected destination path descriptors only to measurements of those exact paths; superseded DuckDB appender evidence is not relabeled.
- Strict benchmark-crate Clippy and formatting pass.
- The repair remains benchmark-owned and does not weaken `EnginePlan::validate_partition_schedule`.

## References

- `.10x/specs/performance-lab-and-envelope.md`
- `.10x/tickets/done/2026-07-16-p3-l3r-isolated-benchmark-child-regression.md`

## Assumptions

- Record-backed: the live smoke failure is `executable engine plan requires compiled source and partition-schedule authority` from `run_cdf_engine_package` after WX1 made that authority mandatory.
- Record-backed: file, Iceberg, REST, and startup benchmark paths already bind their real compiled source plans; only the benchmark-private memory source retained the stale path.

## Journal

- 2026-07-18: The authoritative smoke command built successfully, measured the native Arrow control, then aborted before its first CDF timing cell because the benchmark-private memory source called `execute_to_package` with an unbound engine plan. The earlier L3R ticket exposed the error text but explicitly left stale workload repair elsewhere; this ticket is that bounded owner.
- 2026-07-18: The full benchmark test target then exposed two more stale authorities from the same migration: file cases derived scheduler jobs and physical-byte evidence from the now-empty resident partition vector even though their canonical task set is external, and the generated destination matrix still named the deleted DuckDB Arrow appender after `canonical_segment_scan` became the sole product path. These are gate defects, not product relaxations, and remain within this repair.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
