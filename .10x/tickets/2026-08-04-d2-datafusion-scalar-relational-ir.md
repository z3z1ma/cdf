Status: open
Created: 2026-08-04
Updated: 2026-08-04
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/done/2026-08-03-d1-project-compilation-manifest-core.md`, `.10x/tickets/done/2026-08-04-d1-5a-project-source-resource-input-authority.md`

# D2 DataFusion scalar closure and native relational IR

## Scope

Implement `.10x/specs/datafusion-scalar-relational-ir.md` as one coherent current-schema
replacement:

- replace the current untyped/Boolean-limited expression IR and compiled-expression versions with
  a closed typed scalar IR;
- generically lower fully resolved/coerced pinned-DataFusion scalar expressions under the ratified
  built-in/`Immutable`/known-canonical-Arrow admission predicate;
- represent canonical built-in calls, operators, typed literals/columns, implicit casts, `CAST`,
  and `TRY_CAST` with exact types, nullability, dependency identity, and input dependencies;
- add the initial one-input filter-then-explicit-projection relational IR with output schema,
  aliases/ordinals, and field lineage;
- execute the recorded graph batch-vectorially under CDF schema, memory, cancellation, error, and
  control-field authority while reusing the pinned DataFusion/Arrow scalar implementation where
  admitted;
- preserve ordinary projection/filter pushdown fidelity and CDF residual execution without
  generating source-native SQL;
- update every current producer, consumer, fixture, test, golden, and plan/artifact hash directly;
  and
- prove deterministic reload/binding, DataFusion differential equivalence, and the focused
  performance envelope.

This ticket creates reusable compiler/runtime IR only. D3 later parses project SQL/envelopes,
supplies source locations, consumes the D1.5a inventory, lowers `upstream(...)`, publishes the
manifest, and performs the current-authoring cutover.

## Non-goals

- parsing `.cdf.sql`, the CDF resource envelope, or `upstream(...)` arguments;
- selecting project sources, drivers, resource options, destinations, contracts, or environment
  configuration;
- joins, multiple inputs, aggregates/grouping, windows, sets, recursion, subqueries, sorting,
  limiting, DML/DDL, or arbitrary source-native SQL;
- DataFusion aggregate/window/table functions, UDFs, extensions, `Stable`/`Volatile` or
  ambient-session functions;
- SQL semantic-annotation syntax or project-defined-semantic loading;
- manifest/lock publication, scaffold/add/example changes, or CLI/runtime project-mode cutover;
- source-specific scalar-function or SQL branches;
- per-row interpretation/materialization or runtime semantic reanalysis;
- Python/WASM hooks; or
- any prior-IR reader, migration, fallback executor, deprecated namespace, feature flag, dual
  artifact, or compatibility shim.

## Acceptance criteria

1. One new current scalar IR version represents exact typed columns/literals, canonical operators,
   canonical built-in scalar calls, and implicit/explicit/try casts. Every node records resolved
   Arrow input/output type and nullability plus deterministic column/function dependencies; invalid
   or unresolved nodes fail `Contract`.
2. One new current relational IR version represents exactly one input, optional Boolean filter, and
   ordered explicit projection. It records collision-free output names, ordinals, exact Arrow
   schema/nullability, transitive field lineage, and filter-before-project semantics; no excluded
   relational node has an opaque representation.
3. Production scalar admission is one generic predicate over the compiler's pinned DataFusion
   built-in scalar registry. It admits every fully resolved/coerced `Immutable` built-in whose
   inputs/output are in CDF's canonical Arrow closure and whose behavior has no uncaptured ambient
   authority. Repository/static inspection finds no generic growing match/list of function names.
4. Aggregate, window, table, UDF/extension, `Stable`, `Volatile`, session-dependent, unknown, and
   unrepresentable expressions fail compile-time admission even when their output type is known.
   Diagnostics identify the exact failed gate and retain supplied source location.
5. Function aliases and equivalent coerced graphs lower to canonical function/signature/type
   identity and equal execution hashes while authored provenance remains separate. Plan identity
   includes exact DataFusion 54.0.0/dependency feature tuple plus CDF IR/executor versions; it
   contains no DataFusion plan/debug serialization.
6. DataFusion-resolved implicit casts, explicit `CAST`, and `TRY_CAST` lower to distinct durable
   nodes. Focused matrices prove exact output types/nullability and match DataFusion values,
   overflows, parse errors, and null-on-failure behavior across CDF's admitted Arrow closure.
7. Runtime consumes only typed CDF IR. It performs no SQL parsing, optimizer/simplifier pass,
   overload selection, type inference, cast insertion, or output-schema derivation. Binding the
   recorded canonical function to the pinned batch-vectorized implementation verifies exact
   function/signature/input/output/nullability identity and fails stale mismatches before output.
8. Execution remains batch-vectorized and CDF-owned: input/output schema assertions, memory,
   bounded temporaries, cancellation, scalar broadcasting, deterministic error provenance,
   lineage, control fields, and resulting batches stay under ordinary CDF authority. No per-row
   dynamic dispatch or scalar cell materialization is introduced.
9. Differential/property tests compare representative immutable numeric, string, binary, temporal,
   nested, and null-handling built-in families plus operators/casts against direct pinned
   DataFusion analysis/execution over nulls and edge values. They compare schema, values, nulls,
   and errors, not merely row counts.
10. DataFusion-expanded star projections and aliases lower to explicit output fields. Pass-through
    projection reuses arrays without unnecessary materialization; aliases/collisions/empty
    projection behave exactly as the spec requires.
11. Projection/filter pushdown remains ordinary typed capability negotiation. Unsupported/inexact
    scalar predicates remain native residuals and differential tests prove pushed/residual output
    equivalence; generic code emits no source-native SQL.
12. Control-critical CDF operation/key fields cannot be removed, renamed, reinterpreted, or derived
    through the new graph without separately ratified authority.
13. The old expression and compiled-expression versions are replaced across all current producers,
    consumers, fixtures, tests, and goldens. Stale old artifacts fail with regenerate/recompile
    remediation; searches find no compatibility reader, dual executor, fallback, or legacy shim.
14. Representative projection/filter/function-family execution is within 15% of direct pinned
    DataFusion/Arrow physical-expression execution on the same host/batches, matching the existing
    native-filter roofline convention. Any wider gap is profiled and repaired or explicitly
    returned as a closure blocker.
15. Focused affected-crate tests/checks, strict affected-crate Clippy, formatting, and
    `git diff --check` pass. No whole-workspace suite is run. One independent final adversarial
    review attempts to falsify the admission predicate, durable identity, runtime no-reanalysis
    boundary, casts/nulls/errors, version replacement, and performance evidence.

## References

- `.10x/decisions/datafusion-deterministic-scalar-closure.md`
- `.10x/specs/datafusion-scalar-relational-ir.md`
- `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md`
- `.10x/specs/sql-project-authoring.md`
- `.10x/specs/project-source-resource-layout.md`
- `.10x/specs/types-contracts-normalization.md`
- `.10x/decisions/compiled-fused-streaming-operator-graph.md`
- `.10x/knowledge/net-new-no-compatibility-policy.md`

## Assumptions

- User-ratified: DataFusion remains the SQL parser/resolver/type/coercion/analyzer; D2 uses a
  rule-based closure over every qualifying deterministic built-in scalar rather than a manually
  curated function-name list; known output type is necessary but immutability, built-in provenance,
  captured semantics, canonical identity, and reproducible batch execution are also required.
- Record-backed: DataFusion 54.0.0 is already pinned; existing CDF expression planning already
  round-trips a Boolean subset through DataFusion analysis into durable CDF authority; current
  package/identity operators may reuse Arrow/DataFusion kernels only beneath CDF contracts and
  differential/performance gates.
- Record-backed: Foundation D1/D1.5a, canonical Arrow closure, source pushdown fidelity, memory,
  control-field, artifact replacement, and no-compatibility policies are active authority.
- Mechanical: exact Rust type/module names and whether pinned scalar binding lives in
  `cdf-expression` or `cdf-engine` are executor choices, provided DataFusion types do not leak into
  neutral kernel/runtime/extension/artifact APIs and one obvious D3 consumption seam results.

## Journal

- 2026-08-04: Opened after the user confirmed the recommended rule-based DataFusion scalar closure.
  Source inspection found the current architecture already converts CDF expressions to DataFusion
  54.0.0 for coercion/simplification and lowers back to CDF identity, while `cdf-expression`
  executes only Boolean/logical/null/comparison kernels and Boolean derives. D2 therefore replaces
  the limited IR/executor rather than adding a SQL parser or duplicating DataFusion's function
  catalog. The ticket deliberately keeps D3 SQL/project parsing and current-authoring cutover out of
  scope.

## Blockers

None. Scope, semantics, constraints, and acceptance are complete from the user-ratified decision and
referenced active records.

## Evidence

Pending execution.

## Review

Pending one independent final adversarial review after implementation evidence is complete.

## Retrospective

Pending execution.
