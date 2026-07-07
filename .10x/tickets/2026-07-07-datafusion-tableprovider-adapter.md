Status: blocked
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md, .10x/decisions/datafusion-tier-b-delegation-boundary.md

# Implement the cdf-engine QueryableResource TableProvider adapter

## Scope

Implement the generic internal `cdf-engine` adapter that exposes eligible Tier B `QueryableResource` resources as DataFusion `TableProvider`s while preserving CDF resource semantics, pushdown fidelity, provenance, and package execution constraints.

This ticket is blocked until `.10x/tickets/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md` resolves Arrow/DataFusion type compatibility.

## Acceptance criteria

- A mock or narrowly scoped Tier B resource can be registered and scanned through a real DataFusion `TableProvider` path inside `cdf-engine`.
- The adapter delegates no-I/O scan negotiation to `QueryableResource::negotiate`.
- `Exact`, `Inexact`, and `Unsupported` predicates map correctly to DataFusion filter-pushdown responses.
- `Inexact` and `Unsupported` filters remain residual and are not dropped.
- The adapter supports only specified simple column/literal binary predicates at first and leaves all other expressions residual.
- Limit pushdown is disabled when inexact pushed filters could make source-side limiting semantically unsafe.
- Kernel and resource-authoring APIs remain free of DataFusion types.
- Tests prove the adapter uses actual DataFusion provider/execution APIs rather than only DataFusion-shaped metadata.
- The implementation keeps `crates/cdf-engine/src/lib.rs` thin and places adapter code in focused modules.

## Evidence expectations

- Focused `cdf-engine` tests proving real DataFusion provider execution.
- Tests for exact, inexact, unsupported, unsupported-expression, projection, and limit behavior.
- A review mapping adapter behavior to `.10x/specs/resource-authoring-planning-batches.md`.
- Compile/clippy/test evidence for affected crates.

## Explicit exclusions

No dependency tuple changes, no kernel API DataFusion exposure, no REST-specific provider, no SQL parser expansion beyond the ratified simple predicate subset, no package execution replacement until CDF provenance and `BatchHeader` handling are explicitly covered, no new source pushdown claims, and no false-`Exact` conformance broadening by implication.

## References

- `.10x/decisions/datafusion-tier-b-delegation-boundary.md`
- `.10x/tickets/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md`
- `.10x/research/2026-07-07-datafusion-delegation-pushdown-triage.md`
- `.10x/specs/architecture-layering-runtime.md`
- `.10x/specs/resource-authoring-planning-batches.md`

## Progress and notes

- 2026-07-07: Opened from triage. The adapter is architecturally ratified, but production implementation is blocked by Arrow/DataFusion dependency tuple compatibility.

## Blockers

Blocked on `.10x/tickets/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md`.
