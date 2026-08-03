Status: draft
Created: 2026-08-03
Updated: 2026-08-03

# SQL project authoring and native CDF lowering

## Status and product direction

The user has established a SQL-like project authoring experience as the preferred direction for
CDF's configuration overhaul: project files should resemble explicit SQL resources, DataFusion may
parse/analyze them, and CDF executes an opaque native plan. This draft defines the safe compiler
boundary and a deliberately narrow first language. Exact file layout and statement grammar remain
unratified.

## Purpose

Add a second authoring front-end that lets users define extraction/load resources and bounded
in-flight transformations in SQL-shaped files while preserving every existing CDF authority:

- typed driver/profile options and secret references;
- source discovery/capability truth;
- native Arrow schemas, contracts, semantics, and normalization;
- compiled source/operator/destination identities;
- deterministic packages, receipts, and checkpoints;
- DataFusion's compile-time-only identity boundary.

The SQL front-end is a compiler, not a runtime query engine and not a new scheduler.

## Non-negotiable boundary

```text
authored SQL-shaped resource + typed project/profile metadata
→ CDF parse and source binding
→ DataFusion SQL analysis where useful
→ validation against CDF source/schema/contract/semantic authority
→ deterministic lowering into native CDF IR
→ versioned project compilation manifest
→ ordinary native CDF execution/package/receipt/checkpoint path
```

DataFusion types/plans MAY exist inside the compiler/engine analysis layer. They MUST NOT appear in
kernel/runtime/source public types, serialized execution identity, checkpoint/package formats, or
destination protocols. Runtime MUST NOT reparse, reoptimize, or reinterpret authored SQL.

## Authoring separation

### Typed project/profile authority

The project configuration retains non-relational operational concerns:

- named source connections and secret references;
- named destinations and environment selection;
- state/package roots and retention;
- egress, retry/rate/quota, trust, and destination policies;
- driver-specific capability options validated by driver JSON schemas;
- compiler/dependency/semantic pins.

These MUST NOT be embedded as arbitrary SQL strings or connection literals. SQL resources refer to
named profiles/sources/destinations.

### SQL resource authority

An explicit SQL resource defines:

- one canonical resource id/origin;
- one named source relation in the first language version;
- projection, aliases, deterministic scalar expressions, casts, and filters;
- semantic annotations through the semantic registry;
- resource contract/disposition/cursor/keys either in a typed CDF statement envelope or a typed
  companion declaration;
- optional destination target binding where project policy permits it.

The file path may supply the resource id only if path-to-id normalization and collision behavior is
specified and manifest-recorded. Relying on incidental filenames without validation is forbidden.

## Grammar decision still open

Three viable shapes remain:

1. CDF-owned `CREATE RESOURCE ... AS SELECT ...` envelope, with CDF parsing the envelope and
   DataFusion parsing/analyzing the query body;
2. standard SQL `SELECT`/view-shaped file plus a typed companion metadata file;
3. standard SQL with a small, strictly parsed metadata header.

The supplied external example is illustrative, not ratified:

```sql
CREATE RESOURCE github.issues
FROM SOURCE github
WITH (cursor = updated_at, disposition = 'merge')
AS SELECT id, state, updated_at FROM source WHERE state <> 'spam';
```

The chosen form MUST prove:

- deterministic parsing and helpful source locations;
- DataFusion compatibility for the relational query body;
- typed, schema-validated non-relational options rather than stringly TOML-in-SQL;
- no credentials or secret values;
- explicit resource/source/destination identity;
- canonical formatting/normalization for hashing without changing SQL semantics;
- forward-compatible rejection of unknown CDF clauses.

Exact syntax is a user-visible semantic blocker. No executable parser ticket may choose it by
convention.

## Initial relational language

The first active language SHOULD support only the surface that can lower completely into reviewed
native CDF operators:

- one source relation per resource;
- explicit `SELECT` projection and aliases;
- deterministic literals and scalar expressions from a versioned allowlist;
- explicit Arrow-compatible casts;
- Boolean `WHERE` predicates with three-valued semantics;
- semantic annotations resolved by exact registry version;
- source pushdown negotiation with `Exact`/`Inexact`/`Unsupported` residual recording;
- primary/merge key, cursor, disposition, contract, and execution metadata through the chosen typed
  envelope/companion surface.

The first language MUST reject rather than defer to runtime:

- joins and cross-resource references;
- aggregation, grouping, windows, set operations, recursive queries, and subqueries;
- nondeterministic functions such as random/current-time unless one later contract freezes their
  value in plan identity;
- DML/DDL against external systems;
- stored procedure/function calls;
- arbitrary source-native SQL or aggregation pipelines;
- runtime table discovery expansion;
- unregistered user-defined functions or types;
- row-level Python/WASM calls.

This is not a permanent SQL feature ceiling. It is the smallest complete language consistent with
the active in-flight-transform boundary.

## Native IR expansion

Current `cdf-expression` is insufficient for general SQL projection because it supports Boolean
columns/literals, logical/null/comparison operations, and Boolean derived columns. Before the SQL
front-end can claim a construct, native CDF IR MUST represent and execute it with:

- exact Arrow input/output types and nullability;
- versioned function identity and semantics;
- deterministic canonical serialization/hash;
- compile-time cast/failure behavior;
- native vectorized execution under CDF memory accounting;
- source pushdown/residual fidelity;
- output-schema and field-lineage derivation;
- golden equivalence against DataFusion analysis for the admitted subset.

The compiler MAY use DataFusion to parse, resolve, type, simplify, and optimize. It MUST lower the
result into a closed CDF scalar/relational IR and record both authored-input identity and native
lowered identity. An unstable DataFusion debug string or physical plan is never identity authority.

Unsupported DataFusion expression nodes/functions fail compilation with exact syntax location and
supported alternatives. They do not remain opaque runtime expressions.

## Source and destination binding

- `FROM` names resolve only against the project compilation environment and source-driver catalog.
- One resource binds to one `CompiledSourcePlan`; SQL does not bypass driver option schemas,
  discovery, egress, health, or type policy.
- Projection/filter requests flow through ordinary `QueryableResource`/source pushdown
  negotiation, preserving residual obligations.
- Destination selection/disposition/target compiles through ordinary destination sheets and plan
  validation; SQL cannot issue DDL/DML directly.
- Source-native query features may be exposed only by a separately typed adapter capability with
  truthful output/identity semantics, not by passing arbitrary SQL through the resource language.

## Contracts and semantic types

- Every output field has an exact Arrow type, nullability, source provenance, and optional resolved
  semantic reference before contract compilation.
- SQL casts affect Arrow type; semantic annotations affect meaning and validation through the
  registry. Neither may impersonate the other.
- Contract rules and trust policy remain CDF artifacts, not DataFusion constraints.
- Semantic casts/annotations resolve to versioned definitions and are included in the manifest.
- A SQL transform cannot remove/alter CDC operation/key control fields or other compiler-declared
  control-critical fields.

## Project compilation and manifest

SQL authoring requires `.10x/specs/project-compilation-manifest.md` to be active. Compilation MUST
record:

- exact SQL input bytes/hash and normalized AST hash;
- parser/DataFusion/native function-version tuple;
- source/profile/resource bindings;
- lowered native expression/operator graph and hash;
- output schema, contracts, semantics, destination plan, and lineage;
- pushdown/residual decisions;
- diagnostics and unsupported/excluded constructs;
- template/macro expansion origin if that later capability exists.

Two SQL spellings that normalize to the same semantics MAY share execution identity only if the
canonicalization law is explicit and tested. Authored-origin hashes remain separately inspectable.

## Coexistence and migration

- Existing TOML/YAML declarative resources remain front-end 1 during migration.
- SQL resources are front-end 2.
- Both MUST lower to the same `CompiledResource`, `CompiledSourcePlan`, contract, operator graph,
  destination plan, and manifest structures.
- Runtime/destination code MUST NOT test the authoring front-end kind.
- `cdf add` MAY generate explicit SQL resources once the grammar is active; it must use atomic
  multi-file publication where companion metadata/lock/manifest changes together.
- Conversion tooling must report unsupported declarative features and never silently drop options.

Removing the old declarative front-end is out of scope until feature equivalence, migration, and
artifact compatibility are separately ratified.

## Explicitness, discovery, and templating

Initial SQL authoring has no general Jinja/template runtime.

- Catalog discovery may enumerate resources through existing source discovery authority.
- `cdf add` or a future generate command materializes explicit resource files.
- Wildcard mappings remain compile inputs only where their expansion is captured in the manifest.
- Repeated explicit files are preferred until real duplication establishes a macro requirement.

If macros are later activated, they MUST:

- expand at compile/generate time only;
- use a closed deterministic input model;
- produce canonical rendered resource artifacts;
- record template hash, parameters, expansion tool/version, and each output hash in the manifest;
- expose rendered diffs before acceptance;
- prohibit runtime environment/network/filesystem discovery and secret interpolation.

Runtime string templating is permanently excluded.

## Security and error behavior

- SQL containing a credential/DSN where only named profiles are permitted fails Contract with
  remediation to secret references.
- Unknown source/profile/resource/field/function/semantic references fail compilation before I/O.
- Ambiguous names require qualification; resolution cannot depend on map iteration order.
- Parser/type/lowering errors retain file, line, column, construct, and stable error code.
- Source/destination discovery and execution errors retain adapter provenance.
- DataFusion internal/planning failures are classified through existing error ownership and never
  exposed as unchecked strings.
- Compiler diagnostics and manifest content use the same redaction authority as CLI JSON/human
  output.

## Acceptance scenarios

1. Given a simple one-source SQL resource, compile produces the same native source/operator/
   contract/destination behavior as an equivalent declarative resource.
2. Given projection/filter/cast/alias expressions, DataFusion analysis and native CDF execution
   agree on Arrow schema, null behavior, values, and errors over property-generated batches.
3. Given an inexact source pushdown, the manifest records it and native residual evaluation
   preserves results.
4. Given a join/window/unknown function, compilation fails at the exact source location and no
   runtime plan or external I/O is produced.
5. Given a named source profile, the SQL/manifest contain no resolved secret value.
6. Given two files resolve to the same resource id, compilation fails deterministically before
   manifest publication.
7. Given SQL and declarative equivalents, runtime code receives indistinguishable native artifacts
   and no authoring-kind branch executes.
8. Given a semantic annotation, its exact definition/version/hash and validation/destination
   effects appear in the manifest.
9. Given an attempted transform of `_cdf_op` or a CDC key, compilation fails as control-critical.
10. Given generated resources, every rendered output is explicit, hashed, diffable, and frozen
    before execution.

## Performance requirements

- SQL compilation performance is measured separately from execution throughput.
- Native execution of the admitted SQL subset MUST stay on vectorized Arrow kernels and the
  existing fused operator/memory path.
- No per-row parser, dynamic-language call, boxed expression dispatch in hot loops, or runtime
  DataFusion re-planning is permitted without measured roofline evidence and a separate decision.
- Source/destination direct-library roofline standards remain unchanged by authoring syntax.

## Open blockers

1. Exact file layout and resource-id derivation.
2. Exact CDF statement envelope versus companion metadata grammar.
3. First native scalar function/cast allowlist and IR version.
4. Project/profile model replacing the current Postgres-special-cased destination policy.
5. Manifest activation and offline/refresh compile semantics.
6. Semantic registry activation and SQL annotation grammar.
7. Whether an initial destination binding belongs in SQL or project metadata.

## References

- `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md`
- `.10x/specs/project-compilation-manifest.md`
- `.10x/specs/semantic-type-registry.md`
- `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md`
- `.10x/decisions/compiled-fused-streaming-operator-graph.md`
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/specs/types-contracts-normalization.md`
- `VISION.md` D-1, D-2, D-9, D-19, D-20
