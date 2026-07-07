Status: active
Created: 2026-07-07
Updated: 2026-07-07

# DataFusion Tier B delegation boundary

## Context

`VISION.md` D-1 says every resource implements Arrow-only `ResourceStream`, while pushdown-capable resources additionally implement `QueryableResource`, which `cdf-engine` wraps in a DataFusion `TableProvider`. D-7 adopts DataFusion's `Exact` / `Inexact` / `Unsupported` vocabulary for pushdown fidelity. D-28 requires each CDF minor release to pin one deliberate Arrow/DataFusion dependency tuple.

Active specs preserve the same boundary:

- `.10x/specs/architecture-layering-runtime.md` says the kernel MUST NOT expose DataFusion types and the engine MUST own planning and execution through DataFusion.
- `.10x/specs/resource-authoring-planning-batches.md` says every resource exposes Arrow-only streams, and pushdown-capable resources negotiate scan requests without I/O.

The current engine implementation closed `.10x/tickets/done/2026-07-05-datafusion-engine-planner.md` as an MVP slice. It records DataFusion-shaped explain/operator metadata and uses `TableProviderFilterPushDown` vocabulary, but the actual package execution path is still a CDF-native Arrow loop. Research in `.10x/research/2026-07-07-datafusion-delegation-pushdown-triage.md` found that no current production resource path constructs a real DataFusion `TableProvider`, `ExecutionPlan`, `SessionContext`, or `SendableRecordBatchStream` over CDF resources.

The same research found a dependency-tuple blocker. CDF first-party resource/package types use Arrow `59.0.0`, while latest/current `datafusion 54.0.0` depends on Arrow `58.3.0`. A direct adapter cannot hand CDF Arrow 59 `RecordBatch` values to DataFusion 54 without an Arrow-major bridge.

## Decision

CDF will implement the VISION D-1 deep DataFusion boundary. The current CDF-native execution loop is a temporary fallback/MVP implementation, not the final architectural boundary.

Tier B resources MUST become DataFusion `TableProvider`s through an internal `cdf-engine` adapter once the Arrow/DataFusion dependency tuple is compatible. Kernel APIs and resource-authoring APIs MUST remain Arrow-only and MUST NOT expose DataFusion types.

The generic adapter MUST preserve CDF pushdown semantics:

- It MUST delegate capability and predicate classification to `QueryableResource::negotiate`.
- It MUST NOT invent or upgrade `Exact` pushdown claims.
- It MUST keep `Inexact` and `Unsupported` filters residual.
- It MUST NOT stringify arbitrary DataFusion expressions into CDF predicates.
- It MUST begin with simple, specified column/literal predicates and leave unsupported expressions residual.
- It MUST NOT push a limit into the resource request when any inexact pushed filter could make source-side limiting semantically unsafe.
- It MUST preserve or explicitly carry CDF provenance needed for package execution before it replaces package-producing execution paths.

CDF will not introduce a permanent Arrow-major bridge in the engine hot path. Dependency tuple alignment under D-28 gates production adapter work whenever the current lockfile is not same-major compatible. Acceptable alignment paths are:

- upgrade DataFusion when a compatible release uses the same Arrow major as CDF's first-party Arrow crates;
- or deliberately repin CDF's first-party Arrow crates to DataFusion's Arrow major after the golden-package suite and artifact compatibility review prove the move safe;
- or record a new explicit decision if neither path is available and a temporary bridge is judged worth its maintenance cost.

Until real DataFusion physical execution exists, explain/operator metadata MUST NOT imply that CDF has executed a real DataFusion `TableProvider`/`ExecutionPlan` when it has not.

## Alternatives considered

Ratify the current thin DataFusion boundary.

Rejected. It contradicts VISION D-1, Chapter 5, Chapter 8, and the active architecture spec. It would also leave CDF maintaining a parallel residual-expression engine as features grow.

Implement a generic adapter immediately using an Arrow 58/59 bridge.

Rejected as the default path. It would put IPC/FFI/C Data conversion in the engine hot path before evidence proves the cost and correctness tradeoff. It also normalizes the exact dependency-tuple mismatch D-28 is meant to control.

Promote `QueryableResource` to the base resource trait now.

Rejected. D-1 explicitly keeps Tier A resources simple and makes pushdown optional. The current findings show missing engine adaptation, not that every resource author must learn query negotiation.

Wait indefinitely for upstream DataFusion without opening implementation owners.

Rejected. The dependency-tuple gate should be explicit, and drift in current explain/operator metadata needs a near-term owner.

## Consequences

The current engine MVP remains valid evidence for pushdown vocabulary, no-I/O negotiation, residual reapplication, and package output, but it is not proof of DataFusion physical execution.

`.10x/tickets/done/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md` closed the first D-28 dependency-tuple gate with a time-boxed DataFusion git pin on Arrow 59.1.

`.10x/tickets/done/2026-07-07-datafusion-tableprovider-adapter.md` closed the first generic `QueryableResource` to DataFusion `TableProvider` adapter slice. Replacing package-producing execution paths with DataFusion physical execution remains future work under this decision.

`.10x/tickets/done/2026-07-07-datafusion-execution-honesty.md` owns the completed near-term source/product drift correction where explain/operator metadata named DataFusion nodes that were not actually executed.

Native Arrow/DataFusion Parquet remains governed by `.10x/decisions/native-arrow-datafusion-parquet-policy.md`; this decision does not broaden the `RUSTSEC-2024-0436` exception or authorize new advisory ignores.
