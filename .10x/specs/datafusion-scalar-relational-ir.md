Status: active
Created: 2026-08-04
Updated: 2026-08-04

# DataFusion scalar closure and native CDF relational IR

## Purpose

Define the Foundation D2 compiler/runtime contract that accepts fully analyzed deterministic
DataFusion scalar expressions and lowers them into durable typed CDF projection/filter authority.
This spec is the implementation contract for
`.10x/decisions/datafusion-deterministic-scalar-closure.md` and the scalar/relational subset later
consumed by `.10x/specs/sql-project-authoring.md`.

## Inputs and ownership

D2 consumes:

- one exact input Arrow schema;
- a fully resolved/coerced DataFusion projection and optional Boolean filter produced under the
  pinned DataFusion compiler version/feature set;
- stable source locations for every authored scalar construct when supplied by D3; and
- CDF control-critical field declarations and source pushdown capabilities when attached by later
  project lowering.

D2 MUST NOT parse project SQL files, the CDF resource envelope, or `upstream(...)`; resolve project
paths/sources/drivers; perform source/destination I/O; publish manifests; or expose a new CLI/runtime
authoring mode.

## Artifact model

### Typed scalar IR

The current scalar IR version MUST be replaced with one closed current version capable of
representing:

- input column references bound by stable name plus compile-time Arrow type/nullability;
- exact typed literals supported by the analyzed DataFusion scalar closure;
- canonical built-in scalar-function calls;
- Boolean/comparison/arithmetic operators represented under stable canonical identities rather
  than parser spellings;
- implicit cast, explicit `CAST`, and `TRY_CAST` as distinct typed nodes;
- fully resolved argument/result Arrow types and nullability on every node;
- canonical input-column dependencies and function dependencies; and
- exact DataFusion version plus CDF scalar IR/executor version required to bind the node.

Every node MUST validate recursively. Unknown node kinds, stale versions, unresolved types,
noncanonical aliases, invalid arity/signature, and output/nullability disagreement MUST fail
`Contract` before execution.

The durable representation MUST be ordinary CDF/Arrow/Serde data. It MUST NOT contain DataFusion
`Expr`, logical/physical plan objects, debug strings, optimizer ids, `SessionContext`, function
pointers, trait objects, or opaque serialized engine state.

### Relational IR

The initial CDF relational graph MUST contain exactly:

1. one typed upstream input boundary supplied by later D3 source lowering;
2. zero or one residual Boolean filter over the input schema; and
3. one ordered projection containing explicit output fields and typed scalar expressions.

DataFusion star/wildcard expansion MUST be complete before this boundary. The projection records
explicit output name, type, nullability, ordinal, and field lineage. SQL aliases are output names;
duplicate or ambiguous output names fail. Filter execution precedes projection and uses SQL
three-valued Boolean semantics. Empty projections and non-Boolean filters fail.

Relational IR version and scalar IR version are part of plan identity. Joins, multiple inputs,
aggregates, grouping, windows, sets, recursion, subqueries, sorting, limiting, repartitioning, and
extension nodes are not representable in the initial version and MUST fail rather than survive as
opaque nodes.

## Scalar admission predicate

The compiler MUST traverse the analyzed expression and admit a function only when:

- it resolves from the compiler's built-in DataFusion scalar registry;
- its declared volatility is `Immutable`;
- it is not a UDF/extension/session/source-native function;
- all overload/coercion decisions are complete;
- all argument and result types/nullability are exact and belong to CDF's canonical Arrow closure;
- no uncaptured session/clock/random/environment/I/O property affects its result;
- its canonical identity/signature can be recorded and resolved under the exact pinned dependency
  tuple; and
- the executor supports batch-vectorized binding with identical deterministic errors.

This predicate MUST be generic. Production admission MUST NOT be implemented as an exhaustive or
growing match over individual built-in function names. Function-family-specific code MAY exist only
inside the pinned DataFusion/Arrow implementation or for a documented type-safety/error bridge; it
MUST NOT redefine function semantics.

Aliases MUST lower to the same canonical function identity and execution hash. Different authored
SQL remains distinguishable through the separate authored-input hash.

Known output type alone is not sufficient. `Stable` and `Volatile` functions, unregistered
functions, user functions, aggregates, windows, and table functions fail even when DataFusion can
infer their output type.

## Cast contract

- DataFusion owns cast/coercion analysis at compile time.
- The IR MUST record source type, target type, cast mode (`implicit`, `explicit`, or `try`), result
  nullability, and pinned cast semantics.
- Explicit `CAST` MUST preserve DataFusion's deterministic error behavior.
- `TRY_CAST` MUST preserve DataFusion's null-on-failure behavior.
- Implicit coercions MUST be explicit nodes in durable CDF identity; they may not be re-inferred at
  runtime.
- A cast whose source or target is outside CDF's canonical Arrow closure fails compilation.
- Destination mapping occurs after output-schema derivation and may reject a valid SQL result type
  through the ordinary destination sheet.

## Compilation and canonical identity

Compilation MUST:

1. receive only a resolved/coerced DataFusion projection/filter graph;
2. reject every non-admitted relational/scalar construct with stable location-aware diagnostics;
3. canonicalize function aliases and operator spellings;
4. lower every implicit coercion explicitly;
5. derive exact output schema, nullability, ordinal, and transitive input-field lineage;
6. record canonical function/signature/type/dependency identities;
7. produce deterministic scalar, relational, and output-schema hashes; and
8. emit no DataFusion runtime plan or opaque fallback.

Equivalent analyzed expressions MUST produce equivalent CDF execution identity regardless of
authored function alias, irrelevant whitespace, or implicit-vs-explicit syntax only when DataFusion
analysis proves the exact same typed operation graph. Authored hashes remain distinct.

## Execution

Execution MUST consume only the recorded typed CDF IR and input schema. It MUST NOT receive authored
SQL or repeat SQL parsing, logical optimization, overload selection, type inference, implicit-cast
insertion, or output-schema derivation.

Runtime binding MAY resolve the recorded canonical built-in against the exact pinned DataFusion
registry and invoke its DataFusion/Arrow batch-vectorized implementation. Binding MUST verify exact
function identity, argument types, output type/nullability, and dependency version. Any mismatch is
`Contract` stale-plan failure with recompile remediation.

The CDF operator owns:

- input/output schema checks and record-batch construction;
- memory accounting and bounded temporary allocations;
- cancellation between batch/kernel calls;
- batch-level expression ordering and scalar broadcasting;
- deterministic error classification/provenance;
- field lineage and control-critical field enforcement; and
- the batches that continue to contract/package identity.

Per-row dynamic dispatch, scalar cell materialization, interpreted SQL, network/filesystem access,
or unaccounted unbounded buffers are forbidden. Batch-level expression-node dispatch into
vectorized kernels is permitted.

## Nulls and errors

- Boolean filters use SQL/Arrow three-valued semantics; only `true` retains a row.
- Scalar null propagation follows the resolved DataFusion built-in/cast semantics exactly.
- Compile-time parse/type/admission failures are `Contract` and retain file/line/column when D3
  supplies them.
- Runtime input values that deterministically violate an admitted scalar/cast operation are `Data`
  unless the operation is `TRY_CAST`, in which case the analyzed null semantics apply.
- Registry/version/signature/schema mismatches are stale-plan `Contract` failures with `cdf compile`
  remediation.
- Engine/library failures that violate an admitted binding invariant retain source provenance and
  are `Internal`; they MUST NOT be flattened into a generic SQL string.

## Pushdown and residuals

Scalar admission is independent of source pushdown. A source MAY receive only the existing typed
projection/filter request and MUST return `Exact`, `Inexact`, or `Unsupported` through ordinary
capability negotiation. The relational plan MUST retain every required residual expression and its
identity. Broad built-in admission never generates source-native SQL or bypasses adapter contracts.

## Semantics and control fields

Semantic types annotate the derived Arrow fields after physical type analysis. They do not alter
DataFusion overload resolution or impersonate physical casts. D3 owns SQL semantic syntax.

Projection/filter/cast/function expressions MUST NOT remove, rename, reinterpret, or derive CDF
control-critical operation/key fields except through a separately ratified control-field contract.

## Current-schema transition

CDF is customer zero. D2 MUST replace the prior expression and compiled-expression versions in one
coherent tranche and update all current producers, consumers, tests, fixtures, plans, and goldens.
It MUST NOT add a v1 reader, migration, deprecated function namespace, fallback executor, feature
flag, or dual plan shape.

## Acceptance scenarios

1. Given two aliases for the same immutable built-in and the same fully coerced arguments, lowering
   records one canonical function identity and equal execution hashes while preserving distinct
   authored provenance.
2. Given representative immutable built-ins across numeric, string, binary, temporal, nested, and
   null-handling families whose resolved types are in CDF's Arrow closure, CDF admits and executes
   them without adding their names to a framework allowlist.
3. Given a built-in returning a canonical nested Arrow type, D2 derives and executes the exact
   schema; a destination may subsequently reject it through normal mapping truth.
4. Given `Stable`, `Volatile`, UDF, aggregate, window, table, or unknown functions, compilation fails
   at that node even when its output type is known.
5. Given implicit cast, `CAST`, and `TRY_CAST`, durable nodes distinguish their modes and native
   execution matches DataFusion values, nulls, overflow/parse failures, and output types.
6. Given projection aliases and `SELECT *` already expanded by DataFusion, CDF records explicit
   ordered fields, rejects collisions, and derives exact transitive lineage.
7. Given a Boolean filter plus projection, native execution applies SQL filter-before-project
   semantics and agrees with DataFusion over property-generated batches containing nulls and edge
   values.
8. Given an admitted function requiring an uncaptured session property, compilation rejects it
   rather than reading ambient runtime state.
9. Given a plan whose DataFusion version, function signature, input schema, output type, or
   nullability differs at runtime, binding fails closed with recompile remediation before output.
10. Given a source that cannot push an admitted scalar predicate, the expression remains a CDF
    residual and output equals an unpushed reference.
11. Given a stale scalar/compiled-expression v1 artifact, current code rejects it; repository search
    finds no compatibility reader or dual executor.
12. Given the same analyzed graph twice, canonical scalar/relational/schema hashes are byte-stable
    and contain no DataFusion debug/plan serialization.

## Performance and validation

- Execution is batch-vectorized and memory-accounted.
- Representative projection/filter/function-family cells MUST remain within 15% of direct pinned
  DataFusion/Arrow physical-expression execution on the same batches/host, matching the existing
  native-filter roofline convention. Any wider gap requires profiling and a named decision.
- Pass-through projection MUST not materialize or copy columns unnecessarily.
- Focused differential/property tests cover schema, values, nulls, errors, casts, nested outputs,
  aliases, reload/binding, residuals, and control fields.
- Validation remains affected-crate only during D2; no whole-workspace suite is required for this
  child.

## Explicit exclusions

- SQL file/envelope parsing and `upstream(...)` argument grammar;
- joins, aggregates, grouping, windows, sets, subqueries, recursion, order/limit semantics, and
  multiple inputs;
- UDF/plugin function execution;
- nondeterministic or ambient-session functions;
- semantic annotation syntax;
- source-native SQL generation;
- manifest/lock publication or the project-authoring cutover;
- Python/WASM row or batch hooks; and
- backward compatibility.

## References

- `.10x/decisions/datafusion-deterministic-scalar-closure.md`
- `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md`
- `.10x/specs/sql-project-authoring.md`
- `.10x/specs/types-contracts-normalization.md`
- `.10x/decisions/compiled-fused-streaming-operator-graph.md`
- `.10x/knowledge/net-new-no-compatibility-policy.md`
