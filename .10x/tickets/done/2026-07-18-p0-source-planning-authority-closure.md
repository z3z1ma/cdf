Status: done
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-18-p0-post-iceberg-integration-stabilization.md
Depends-On: .10x/tickets/done/2026-07-18-p0-external-partition-authority.md, .10x/tickets/done/2026-07-18-p0-typed-compiled-source-identities.md

# P0: close source planning authority seams

## Scope

Finish the external-task migration so source extensions consume one closed planning authority instead of representation-sensitive helpers. Remove public post-construction authority replacement and silent inline-only mutation, preserve file-manifest summary evidence across external drain epochs, and provide one source-SDK planning entrypoint whose bounded/high-cardinality choice is explicit rather than adapter folklore.

## Non-goals

- Replacing source-owned partition semantics with generic inference.
- Materializing external task sets for diagnostics or summaries.
- Adding speculative source drivers.

## Acceptance Criteria

- Identity-bearing partition authority cannot be replaced through a public mutable setter after `ScanPlan` construction.
- Any partition transformation handles inline and external authority explicitly; it never silently no-ops.
- External drain epochs preserve typed file-manifest summary evidence without task-set enumeration.
- A new source adapter has one documented/compiler-enforced path for bounded inline versus external task planning.
- Extension-boundary conformance rejects representation-dependent adapters.

## Assumptions

- Record-backed: invalid inline/external states were removed, but mutation and representation-sensitive helpers can recreate the same failure class.
- Record-backed: source semantics remain source-owned; generic layers validate authority rather than manufacture it.

## Journal

- 2026-07-18: Activated after closed partition authority, typed source identity, and portable `u64`
  cardinality. The remaining seam is not the sum type itself: public post-construction replacement,
  a silent inline-only mapper, external drain summaries derived from resident partitions, and
  adapter-specific knowledge of when to externalize planning still permit representation-dependent
  extensions.
- 2026-07-18: Replaced `ScanPlan::new` with the deliberately explicit
  `ScanPlan::from_partition_authority`. Removed `replace_partition_authority` and
  `map_inline_partitions`; the only authority transformation now consumes a complete plan and
  requires an exhaustive `PartitionAuthority` match. Resume binding likewise consumes and returns
  a complete plan through the source boundary, so adapters cannot mutate authority behind the
  engine's schedule and explain joins.
- 2026-07-18: Kept zero-task Glue and Iceberg resume plans external rather than changing their
  representation to an empty inline vector. Files, Glue, and Iceberg now rewrite their own task
  artifacts and return one complete source-authored plan. File resume planning also recalculates
  `planned_source_bytes` from the selected files instead of retaining the original full-inventory
  estimate.
- 2026-07-18: Made schema-evidence planning explicitly match both authority variants. Inline
  partition metadata remains compiler-bound; external task observation bindings remain
  source-authored and are validated incrementally by the registry reader without task enumeration.
- 2026-07-18: External file drain summaries now derive from the typed incremental shape plus the
  canonical `u64` partition cardinality. A regression test uses more than `u32::MAX` tasks and
  proves the summary path does not materialize the task set.
- 2026-07-18: Fresh-hat review caught an avoidable O(N) clone in the first immutable resume API.
  The final API consumes `ScanPlan` and `EnginePlan`, preserving the authority boundary without
  copying resident partition vectors. The final full workspace gate passed after this correction.

## Blockers

None.

## Evidence

- Public mutable replacement is absent: `rg 'replace_partition_authority|map_inline_partitions|ScanPlan::new' crates --glob '*.rs'` returned no matches. All source and fixture planners use the one explicit constructor.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo check --workspace --all-targets` passed after the consuming resume API migration.
- Targeted authority behavior passed: external >`u32` file drain summary, multi-file manifest incremental/no-op, Glue execute/resume, Iceberg append-snapshot selection and zero-task external authority, plus both generic external-source conformance laws.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo clippy --workspace --all-targets --all-features -- -D warnings` passed.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo nextest run --workspace --all-features` passed 1,777/1,777 tests with 40 explicit skips in 232.933 seconds. The 100-repeat DuckDB and Parquet destination goldens and jobs-invariance tests were green.

## Review

Fresh-hat verdict: pass. The review traced every former authority mutation call, every source
resume implementation, the window-scoping decorator, engine schedule recompilation, external
manifest summaries, and the extension fixture. One significant performance finding—the first
draft's full inline-plan clone—was corrected before the final gate by changing the transformation
to consume ownership. No critical or significant findings remain. Residual risk is limited to
source-specific semantic errors inside a new adapter's external task encoder; registry validation
and extension conformance remain the owning defenses for that intentionally source-private code.

## Retrospective

The invalid-state enum was only half the boundary while callers could mutate or silently skip one
variant afterward. The durable fix was not another guard but an ownership protocol: choose the
closed authority once, then consume and return a whole plan for source-owned rebinding. Preserving
external representation for zero tasks also removed a recurring special case from schedulers and
decorators. Fresh review must include allocation behavior even for correctness refactors; immutable
APIs can still regress high-cardinality planning if implemented by cloning.
