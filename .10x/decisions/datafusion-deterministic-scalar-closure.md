Status: active
Created: 2026-08-04
Updated: 2026-08-04

# DataFusion deterministic scalar closure under CDF plan authority

## Context

Foundation D uses SQL-shaped resource files, DataFusion analysis, and ordinary CDF execution. The
existing boundary correctly prevents transient DataFusion plans and debug representations from
becoming package/replay identity, but the initial D2 checkpoint was phrased as a manually selected
scalar/cast allowlist. That wording could be misread to require CDF to reimplement individual
DataFusion functions or arbitrarily expose only a small named subset.

The user clarified that DataFusion is expected to parse and analyze SQL and that CDF should ideally
support every DataFusion scalar function whose result has a known type, excluding aggregates and
the other already-rejected relational families. The confirmed recommendation preserves that broad
surface while adding the gates a known output type alone cannot provide: deterministic semantics,
built-in provenance, canonical identity, reconstructible vectorized execution, and exact
dependency/version authority.

Current source already proves the architectural seed:

- `cdf-engine::expression` converts CDF expressions to DataFusion `Expr`, asks DataFusion 54.0.0 to
  coerce/simplify them, and lowers the result back into serialized CDF expression authority;
- `cdf-expression` executes the current Boolean subset through Arrow vector kernels;
- `cdf-contract` pins DataFusion 54.0.0 and expression-plan identities;
- the current IR cannot represent general typed scalar outputs, casts, projections, or broad
  built-in functions, so D2 is a real current-schema replacement rather than a parser-only change.

This decision refines `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md`. It does
not supersede the rule that DataFusion plans/types remain outside kernel, runtime, package,
checkpoint, and extension APIs, nor does it authorize DataFusion to own package bytes or replay
identity.

## Decision

### DataFusion owns SQL scalar analysis

The D3 compiler MUST use the pinned DataFusion SQL/analyzer stack for the relational query body:
parsing, name and function resolution, overload selection, type inference, implicit coercion,
nullability analysis, simplification, and output-schema derivation. CDF MUST NOT grow a parallel SQL
parser, scalar type-coercion lattice, or manually duplicated built-in-function catalog.

CDF owns the surrounding resource envelope and the path/configuration/driver binding established by
Foundation D1.5. `upstream(...)` remains a CDF/driver-owned typed relation boundary rather than an
arbitrary DataFusion table function.

### Scalar admission is a closed rule, not a name list

For the exact DataFusion version and feature set pinned by the compiler, CDF MUST admit a resolved
scalar expression when and only when all of the following hold:

1. every function is a registered DataFusion built-in scalar function, not an aggregate, window,
   table function, user-defined function, extension function, stored routine, or source-native
   expression;
2. every function is classified `Immutable`, and its exact behavior needs no ambient clock,
   randomness, process/environment state, locale, session property, external I/O, or other value
   absent from typed plan identity;
3. DataFusion has fully resolved/coerced every argument and produced exact input Arrow types, output
   Arrow type, and nullability;
4. every input/output type belongs to CDF's canonical Arrow closure and can cross the ordinary
   schema/package/destination-planning boundaries; a particular destination may still reject the
   resulting schema through its normal destination sheet;
5. the expression can be represented in the current typed CDF scalar IR with canonical function,
   signature, cast, literal, column, type, nullability, and dependency identities;
6. the pinned implementation can be bound and invoked batch-vectorially under CDF memory,
   cancellation, error, and control-field authority without runtime SQL parsing, optimization, type
   inference, or semantic replanning; and
7. differential conformance proves the CDF-owned operator produces the same schema, values, nulls,
   and deterministic errors as the analyzed DataFusion expression.

This is a rule-based closure over the compiler's registered built-ins. Generic compiler code MUST
NOT carry a growing match/list of individually blessed function names. A function that fails any
gate is rejected at compilation with its exact location and failed admission reason.

Function aliases are authored syntax only. The durable CDF node records DataFusion's canonical
built-in identity, pinned DataFusion version, resolved argument types, output type/nullability, and
CDF IR/executor version. The authored SQL byte/hash remains separate provenance.

### Casts are typed scalar nodes

DataFusion-resolved implicit casts, explicit `CAST`, and `TRY_CAST` MAY enter the same closure when
their source and target types belong to CDF's canonical Arrow closure. The CDF IR MUST distinguish
implicit, erroring explicit, and null-on-failure casts, record both types, and preserve DataFusion's
exact overflow/parse/null semantics. Destination mapping is a later ordinary schema-planning gate,
not a reason to make SQL typing destination-specific.

### CDF owns the durable and runtime envelope

DataFusion logical/physical plan objects, `Expr` serialization, debug strings, optimizer node ids,
and session catalogs MUST NOT enter durable CDF identity. Compilation lowers the fully analyzed
projection/filter expression graph into a closed typed CDF scalar/relational IR and records the
exact DataFusion/CDF dependency tuple.

At runtime, CDF owns operator order, input/output schema assertions, field lineage, memory
accounting, cancellation, source pushdown/residual truth, control-critical columns, package bytes,
receipts, and replay. A CDF operator MAY call the pinned DataFusion/Arrow vectorized scalar
implementation beneath that envelope. Resolving the recorded canonical built-in and binding
recorded typed columns is execution binding, not permission to reparse, reoptimize, infer different
types, or reinterpret SQL. The runtime MUST fail closed if the resolved implementation/signature/
output does not exactly match the recorded binding.

Batch-level expression-node dispatch is permitted; per-row dynamic dispatch, scalar materialization,
or authored-SQL interpretation is not. Performance remains governed by direct DataFusion/Arrow
kernel comparison and the existing CDF memory/accounting path.

### Relational closure remains narrow

The initial relational IR admits exactly one already-bound upstream input, Boolean filtering, and
explicit projection/aliases after DataFusion star expansion. It rejects joins, cross-resource
references, aggregation/grouping, windows, set operations, recursive queries, subqueries,
ordering/limit semantics not separately ratified, DML/DDL, and arbitrary source-native SQL. Broad
scalar closure does not broaden the relational closure.

Source pushdown may consume only expressions a source explicitly proves `Exact` or `Inexact` under
the ordinary capability contract. Every other admitted scalar expression stays a CDF residual;
generic scalar admission never implies adapter pushdown.

### Current-schema replacement

D2 MUST replace the current scalar/compiled-expression artifact versions outright and update all
current producers, consumers, fixtures, and goldens coherently. There is no reader, migration,
fallback, alias layer, or dual executor for the prior IR version.

## Alternatives considered

### Maintain a small named function allowlist

Rejected. It duplicates DataFusion's registered capabilities, makes ordinary deterministic
functions land through repeated framework edits, and encourages CDF-specific SQL behavior. A closed
admission predicate is both broader and more rigorous.

### Admit any expression with a known output type

Rejected as insufficient. Volatile/stable/session-dependent/UDF expressions can have known types
while remaining nondeterministic, externally extensible, or unreconstructible during replay.

### Serialize or execute the DataFusion logical/physical plan as CDF identity

Rejected. Those representations are not CDF's stable artifact contract, leak DataFusion types
through neutral boundaries, and would permit dependency upgrades or runtime optimization to change
identity-bearing output without an explicit CDF schema transition.

### Reimplement every admitted scalar function in CDF

Rejected. It recreates a large, subtle function/type/null/error surface, delays common functions,
and increases differential drift. CDF should own the typed operator envelope and reuse the pinned
vectorized implementation.

### Let DataFusion execute the entire query/data plane

Rejected. It would cross the established native package/verdict/segmentation authority and make
broader relational scheduling behavior part of identity. Only scalar implementations beneath the
closed CDF operator envelope are admitted here.

## Consequences

- D2 becomes a typed IR/executor/admission project, not a hand-authored function backlog.
- A pinned DataFusion upgrade can broaden/change the available closure only through an explicit CDF
  dependency/artifact transition and differential evidence.
- Common deterministic scalar functions, including complex/nested-output built-ins, are available
  when their resolved Arrow types fit CDF's canonical closure; destinations retain independent
  schema truth.
- Runtime execution may reuse DataFusion/Arrow scalar kernels without making a transient DataFusion
  plan durable authority.
- D3 remains responsible for the CDF envelope, SQL file diagnostics, `upstream(...)`, semantic
  syntax, and current-authoring cutover. D2 owns no user-selectable project mode.
- The broad closure increases conformance-matrix importance: function families, casts, nulls,
  aliases, errors, nested outputs, and reload/rebinding must be tested generically rather than by a
  few happy-path names.
