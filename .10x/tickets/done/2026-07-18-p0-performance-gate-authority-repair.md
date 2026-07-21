Status: done
Created: 2026-07-18
Updated: 2026-07-21
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
- 2026-07-18: The repaired smoke advanced beyond the engine cell and exposed a second pre-WX1 fixture at package replay: the hand-built package omitted the mandatory validation-program and related compiler artifacts. Replay fixture construction now executes a real in-memory resource through Tier A planning, compiled-source/operator-graph binding, engine package production, state/commit preimages, verification, and the same staging-lease authority injected by project execution. The archive-only fixture was renamed so it cannot be mistaken for an executable replay package.
- 2026-07-21: Destination-fidelity expansion correctly bumped all three schema-preflight versions and exposed a stale benchmark catalog. Audit then found the claimed "exact" envelope join compared only destination id, path id, and a prose-managed evidence version. Destination observations now bind the exact current preflight version plus a canonical SHA-256 over every execution-affecting descriptor field. Preflight-only changes therefore do not relabel retained performance samples, while any tuning, topology, ordering, fallback, ingress, writer, or sizing drift invalidates the join.
- 2026-07-21: Replaced the ambiguous PostgreSQL/Parquet `unsupported-arrow-v1` cells with concrete remaining boundaries. PostgreSQL rejects only the invalid `Time32(Microsecond)` Arrow pairing in this matrix; its ordinary nested, union, dictionary, duration, interval, and run-end-encoded values remain allowance-gated JSONB. Parquet uses the concrete unsupported `Interval(MonthDayNano)` fixture. Current conformance executes both facts rather than preserving stale expectations that newly supported schemas remain ineligible.

## Blockers

None.

## Evidence

- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-benchmarks --tests --locked -j 12` — passed: 42 tests executed, 41 passed and the live-Postgres cell was explicitly ignored; both current-authority engine/package regressions passed.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 CDF_BENCH_SUITE=smoke cargo bench -p cdf-benchmarks --bench baseline --locked -j 12` — passed all six smoke cells on the final tree. Five cells reported no statistically significant change; `trend.cdf_package_replay.duckdb_package_receipt_checkpoint.medium` improved to `[260.76 ms, 377.98 ms]` with `p = 0.01`. These laptop Criterion intervals prove completion/no regression only, not product throughput.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-conformance --lib destination_catalog --locked -j 12` — passed 7/7 current catalog, extension-boundary, payload, and exact eligible/ineligible preflight scenarios.
- The generated envelope golden passes only when every destination observation matches destination/path/evidence, the canonical execution-descriptor digest, the schema-preflight version, eligibility, and schema fixture. The negative law independently corrupts the execution digest and proves generation fails; superseded DuckDB appender evidence remains historical and cannot join the current catalog.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli --lib benchmark_destination_catalog_artifact_matches_the_product_registry --locked -j 12` — passed and proves the checked benchmark artifact remains an exact projection of the product registry after all three preflight-version changes.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-bench-core -p cdf-benchmarks -p cdf-conformance -p cdf-cli --all-targets --all-features --locked -j 12 -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check` — passed.

## Review

Pass. The repair introduces no product data-plane branch, fallback, or validation bypass and does not touch `EnginePlan::validate_partition_schedule`. Compiler/package evidence is generated by the live engine; synthetic cursor/state authority remains confined to the benchmark-owned memory resource. The destination join no longer trusts human-coordinated version labels as a complete descriptor identity: execution mechanics are content-addressed and preflight semantics are independently version-bound. The split deliberately retains old EC2 timings only when execution-affecting fields are byte-for-byte equivalent; it does not rename or normalize samples. The only non-benchmark source change corrects a stale conformance fixture to exercise actual current unsupported boundaries, with no product behavior change. Full tests, negative authority tests, strict Clippy, and the release smoke falsify the known stale-authority and performance-regression modes.

Residual risk: EC2 samples remain the previously recorded one-sample current-path measurements and are not fresh distribution evidence. That limitation is explicit in the destination evidence record and remains owned by P3 envelope closure; this ticket only proves that the report cannot attach those samples to a different execution descriptor or preflight contract.

## Retrospective

- A prose-managed evidence version is not a content identity. It may remain a human-readable measurement epoch, but machine joins need a canonical digest of the mechanics whose performance the sample represents.
- Schema support and execution performance evolve independently. Combining them in one version either invalidates valid timing evidence unnecessarily or encourages relabeling old measurements. Separate execution-descriptor identity from schema-preflight identity and require both at the final envelope join.
- Broad labels such as `unsupported-arrow-v1` conceal both progress and stale tests. Concrete schema fixtures make the remaining boundary reviewable and prevent a newly supported type from continuing to appear as a green rejection.
- A release benchmark with fat LTO is intentionally slow to build, but it caught no regression and confirmed the repaired smoke reaches every cell. The ordinary test and Clippy graph remains fast enough for iteration; release smoke belongs at tranche closure, not every edit.
