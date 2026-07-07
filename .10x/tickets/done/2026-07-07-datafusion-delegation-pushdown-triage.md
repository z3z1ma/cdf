Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-performance-investigation-backlog.md

# Triage DataFusion delegation and pushdown performance

## Scope

Investigate whether CDF delegates enough query/filter/projection/sort/limit work to DataFusion and source pushdown layers, or whether current engine-side loops leave performance on the table for workloads DataFusion can optimize.

This ticket was triage only. It did not authorize planner rewrites, DataFusion `TableProvider` additions, predicate-language expansion, or changes to pushdown fidelity semantics.

## Current hypothesis

CDF's architecture treats DataFusion as core, but not every resource execution path necessarily flows through optimized DataFusion plans. The engine can apply residual filters and projections itself over Arrow batches, which is correct and simple, but may underuse DataFusion for complex predicates, file scans, Parquet row-group pruning, projection pruning, limits, joins, or future transformations.

## Investigation questions

- Which current resource paths use DataFusion directly, and which only use CDF-native batch loops?
- Are residual filters intentionally simple, or are they becoming a parallel query engine by accident?
- Which pushdowns are already negotiated: projection, filters, limit, ordering, partitioning?
- Does DataFusion's native Parquet reader now participate in CDF file source planning where appropriate?
- Would adding DataFusion `TableProvider` wrappers for CDF resources improve performance or blur resource/runtime boundaries?
- How should CDF preserve exact/inexact/unsupported predicate fidelity while delegating more work?
- Which transformations belong in DataFusion expressions versus contract validation versus source-specific execution?

## Candidate validation scenarios

- Predicate-heavy file scan where DataFusion can prune or vectorize better than residual engine loops.
- Projection-heavy wide schema scan.
- Limit pushdown where early termination avoids package writes.
- Parquet file scan with row-group pruning if stats are available.
- REST resource with inexact cursor pushdown plus residual filtering, to verify DataFusion is not the right layer for network/API pagination.

## Acceptance criteria

- Inventory DataFusion use in `cdf-engine`, `cdf-formats`, declarative resources, and package/archive paths.
- Identify which performance gaps are due to missing pushdown claims versus missing DataFusion delegation versus source limitations.
- Recommend no action, a focused planner enhancement, a source-specific `TableProvider`, a predicate-language spec update, or a benchmark first.
- If implementation is recommended, open a child ticket scoped to one pushdown or one resource type, not a generic "use DataFusion more" rewrite.
- Preserve the active rule that `Exact` pushdown claims must be conformance-tested and false exactness is a correctness bug.

## Evidence expectations

- Source inspection of `crates/cdf-engine/src/planning.rs`, `predicates.rs`, `execution.rs`, `crates/cdf-formats/**`, and declarative resource planning.
- Relevant DataFusion API/version inspection if needed.
- A comparison sketch between direct DataFusion and CDF-packaged execution for at least one representative query, if later activated.

## Explicit exclusions

No query planner rewrite, no broad predicate parser, no join/SQL transformation layer, no REST `TableProvider`, no unratified exact pushdown claim, no dependency upgrade, and no implementation before triage recommendation.

## References

- `.10x/tickets/2026-07-07-performance-investigation-backlog.md`
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/decisions/native-arrow-datafusion-parquet-policy.md`
- `.10x/tickets/done/2026-07-05-datafusion-engine-planner.md`
- `crates/cdf-engine/**`
- `crates/cdf-formats/**`
- `crates/cdf-declarative/**`

## Progress and notes

- 2026-07-07: Opened from performance discussion. CDF should not claim DataFusion-level speed for paths that do not actually delegate work to DataFusion.
- 2026-07-07: Completed read-only triage in `.10x/research/2026-07-07-datafusion-delegation-pushdown-triage.md`. The current source uses DataFusion fidelity vocabulary and DataFusion-shaped operator metadata, but no current production path executes CDF resources through real DataFusion `TableProvider`s or physical plans.
- 2026-07-07: Ratified the architectural boundary in `.10x/decisions/datafusion-tier-b-delegation-boundary.md`: CDF will implement VISION D-1 deep DataFusion delegation, but production adapter work is gated on Arrow/DataFusion dependency tuple compatibility and must preserve CDF pushdown semantics.
- 2026-07-07: Opened follow-up owners:
  - `.10x/tickets/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md`
  - `.10x/tickets/2026-07-07-datafusion-tableprovider-adapter.md`
  - `.10x/tickets/done/2026-07-07-datafusion-execution-honesty.md`
- 2026-07-07: Closure review recorded in `.10x/reviews/2026-07-07-datafusion-delegation-pushdown-triage-review.md`.

## Blockers

None for triage closure. Implementation follow-up blockers are owned by `.10x/tickets/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md` and `.10x/tickets/2026-07-07-datafusion-tableprovider-adapter.md`.
