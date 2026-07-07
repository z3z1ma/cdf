Status: done
Created: 2026-07-07
Updated: 2026-07-07

# DataFusion delegation and pushdown triage

## Question

Does CDF currently delegate enough planning and execution work to DataFusion, and should the system ratify the current thin boundary or implement the generic `TableProvider` adapter described by `VISION.md`?

## Sources and methods

- Inspected `VISION.md` D-1, D-7, D-28, Chapter 5, and Chapter 8.
- Inspected active specs:
  - `.10x/specs/architecture-layering-runtime.md`
  - `.10x/specs/resource-authoring-planning-batches.md`
- Inspected prior engine ticket, evidence, and review:
  - `.10x/tickets/done/2026-07-05-datafusion-engine-planner.md`
  - `.10x/evidence/2026-07-06-datafusion-engine-planner.md`
  - `.10x/reviews/2026-07-06-datafusion-engine-planner-review.md`
- Inspected native Parquet and supply-chain records:
  - `.10x/decisions/native-arrow-datafusion-parquet-policy.md`
  - `.10x/research/2026-07-06-native-parquet-paste-risk.md`
- Inspected current source:
  - `crates/cdf-engine/src/planning.rs`
  - `crates/cdf-engine/src/execution.rs`
  - `crates/cdf-engine/src/predicates.rs`
  - `crates/cdf-engine/src/types.rs`
  - `crates/cdf-kernel/src/resource.rs`
  - `crates/cdf-formats/src/resource.rs`
  - `crates/cdf-formats/src/readers.rs`
  - `crates/cdf-declarative/src/compiled.rs`
  - `crates/cdf-declarative/src/rest_runtime.rs`
  - `crates/cdf-declarative/src/sql_runtime.rs`
  - `crates/cdf-dest-postgres/src/source.rs`
  - `crates/cdf-package/src/builder.rs`
  - `crates/cdf-package/src/ops.rs`
  - `crates/cdf-package/src/parquet.rs`
- Inspected current dependency graph and registry metadata with:
  - `cargo tree -p cdf-engine --locked -i arrow-array@59.0.0`
  - `cargo tree -p cdf-engine --locked -i arrow-array@58.3.0`
  - `cargo tree -p cdf-engine --locked -i datafusion@54.0.0`
  - `cargo info datafusion`
  - `cargo info arrow-array`
  - `cargo info parquet`
  - `cargo search datafusion --limit 3`
  - `cargo search arrow-array --limit 3`
- Delegated read-only inspections to three explorer subagents:
  - Current DataFusion use across engine/formats/declarative/package paths.
  - Pushdown-fidelity semantics and false-`Exact` guardrails.
  - Feasibility of a generic DataFusion `TableProvider` adapter against the current lockfile.

## Findings

`VISION.md` and active specs do not ratify a permanently thin DataFusion boundary. D-1 says Tier B resources become DataFusion `TableProvider`s through a generic adapter in `cdf-engine`; Chapter 5 says `cdf-engine` adapts kernel Arrow streams to DataFusion streams and owns expression evaluation, planning, execution, pushdown negotiation, and `EXPLAIN`. The active architecture spec repeats that Layer 2 MUST own planning and execution through DataFusion while keeping the kernel DataFusion-free.

The current source does not yet implement actual DataFusion execution for CDF resources. `cdf-engine` depends on `datafusion` and maps `PushdownFidelity` into `TableProviderFilterPushDown`, but execution drains `ResourceStream::open` directly, applies residual filters through `cdf-engine/src/predicates.rs`, applies limit/projection in Rust loops, then writes packages through `cdf-package`. The serialized operator chain currently contains `DataFusionTableProvider` and `DataFusionScanExec`, but those are metadata names, not actual `TableProvider` or `ExecutionPlan` instances.

Concrete resource paths are CDF-native today. `cdf-formats::FileResource` implements `ResourceStream` only. Declarative REST and SQL resources implement `QueryableResource`, but their negotiation and execution are native CDF/resource code: REST pages HTTP and builds Arrow batches; table-backed SQL delegates projection/filter/order/limit to Postgres SQL and converts rows to Arrow batches. Package/archive paths read Arrow IPC or write native arrow-rs Parquet; they do not ask DataFusion to plan file/package scans.

The pushdown-fidelity contract is usable for a DataFusion adapter if the adapter preserves, rather than strengthens, resource claims. `Exact` may drop the engine filter; `Inexact` and `Unsupported` must stay residual. A generic adapter must delegate classification to `QueryableResource::negotiate`, must not infer `Exact`, and must not stringify arbitrary DataFusion expressions into CDF predicates. The first adapter slice should support only well-defined column/literal binary comparisons and leave complex expressions residual.

False-`Exact` protection exists but remains incomplete relative to the active spec. Current conformance structurally checks classification and has negative tests for dishonest classification, and Postgres has live checks for exact structured predicate pushdown. The spec still requires adversarial null/timezone/collation cases as the conformance bar for exactness across resources.

The current lockfile blocks a small, direct hot-path adapter. CDF first-party crates use Arrow `59.0.0`, while latest/current `datafusion 54.0.0` uses Arrow `58.3.0` internally. A CDF `arrow_array::RecordBatch` and `arrow_schema::SchemaRef` cannot be returned directly to DataFusion 54 `SendableRecordBatchStream` or `TableProvider::schema` without crossing Arrow major type identity. `cargo search` and `cargo info` on 2026-07-07 report `datafusion 54.0.0` and `arrow-array 59.0.0` as current.

The native Arrow/DataFusion Parquet decision ratifies accepting the `paste` advisory path for native Parquet, but it does not ratify an Arrow 58/59 execution bridge or a dependency-tuple mismatch as the permanent engine boundary. D-28 says each CDF minor pins one Arrow/DataFusion dependency tuple deliberately and gates upgrades on the golden package suite.

The current explain/operator metadata is honest as an intended architecture marker only if the reader already knows the MVP limitation. For cold readers and product surfaces, naming current CDF-native execution nodes as `DataFusionTableProvider`/`DataFusionScanExec` overstates implementation state.

## Conclusions

Ratifying the current thin boundary is rejected. The active product architecture remains DataFusion-deep: Tier B `QueryableResource` implementations should become DataFusion `TableProvider`s through `cdf-engine`, while resource authors and kernel APIs remain Arrow-only.

Implementation must be sequenced through dependency-tuple alignment before a production hot-path adapter. A permanent Arrow 58/59 IPC, FFI, or C Data bridge inside the engine execution hot path would add complexity exactly where D-1 and D-28 are meant to keep the system coherent.

The next durable owners are:

- `.10x/decisions/datafusion-tier-b-delegation-boundary.md`
- `.10x/tickets/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md`
- `.10x/tickets/2026-07-07-datafusion-tableprovider-adapter.md`
- `.10x/tickets/2026-07-07-datafusion-execution-honesty.md`

The adapter should initially prove the boundary with a mock or narrowly bounded resource after Arrow/DataFusion type compatibility is settled. It should not replace package execution until it can preserve `BatchHeader` provenance, source positions, package identity, and checkpoint evidence.
