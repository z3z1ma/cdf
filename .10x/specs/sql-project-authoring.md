Status: active
Created: 2026-08-03
Updated: 2026-08-04

# SQL project authoring and native CDF lowering

## Status and product direction

The user has established a SQL-like project authoring experience as the preferred direction for
CDF's configuration overhaul: project files resemble explicit SQL resources, DataFusion may
parse/analyze them, and CDF executes an opaque native plan. On 2026-08-04 the user superseded the
explicit-id/profile model with filesystem-derived source/resource identity and one typed shared
source configuration in `cdf.toml`, governed by
`.10x/decisions/filesystem-source-resource-and-configuration-authority.md` and
`.10x/specs/project-source-resource-layout.md`.

## Purpose

Add a second authoring front-end that lets users define extraction/load resources and bounded
in-flight transformations in SQL-shaped files while preserving every existing CDF authority:

- typed shared-source/driver options and secret references;
- source discovery/capability truth;
- native Arrow schemas, contracts, semantics, and normalization;
- compiled source/operator/destination identities;
- deterministic packages, receipts, and checkpoints;
- DataFusion's compile-time-only identity boundary.

The SQL front-end is a compiler, not a runtime query engine and not a new scheduler.

## Non-negotiable boundary

```text
authoritative source/resource path + authored SQL resource + typed effective source configuration
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

### Typed project/source authority

Root `cdf.toml` retains non-relational operational concerns:

- named configured sources, their immutable source types, shared connection options, and secret
  references;
- named destinations and environment selection;
- state/package roots and retention;
- egress, retry/rate/quota, trust, and destination policies;
- driver-specific capability options validated by driver JSON schemas;
- compiler/dependency/semantic pins.

These MUST NOT be embedded as arbitrary SQL strings or connection literals. A resource does not
refer to its configured source by name: `sources/<source>/<resource>.cdf.sql` binds it before SQL is
parsed. SQL may refer to logical destination targets as separately governed.

### SQL resource authority

An explicit SQL resource defines:

- one driver-owned upstream relation/selector in the first language version;
- projection, aliases, deterministic scalar expressions, casts, and filters;
- semantic annotations through the semantic registry;
- resource contract/disposition/cursor/keys either in a typed CDF statement envelope or a typed
  companion declaration;
- optional destination target binding where project policy permits it.

The authoritative path supplies source name, resource name, and canonical resource id exactly as
specified by `.10x/specs/project-source-resource-layout.md`. SQL MUST NOT repeat or override them.
Path validation, collision behavior, renames, and manifest evidence are compiler contracts, so the
identity is filesystem-derived but never incidental.

## Ratified grammar boundary

Each `sources/<source>/<resource>.cdf.sql` file contains exactly one CDF-owned resource statement.
The path declares canonical id `<source>.<resource>`; the statement cannot declare an id or source.
CDF parses the typed envelope and DataFusion parses/analyzes only the `SELECT` body.

The prior example repeated `CREATE RESOURCE github.issues` and `FROM SOURCE github`. Both are
retired. The envelope instead has this structural ownership:

```sql
CREATE RESOURCE
<RELATION CLAUSE> public.issues
TARGET warehouse.issues
DISPOSITION MERGE
MERGE KEY (id)
CURSOR updated_at
TRUST GOVERNED
AS
SELECT id, state, updated_at
FROM source
WHERE state <> 'spam';
```

`<RELATION CLAUSE>` is a grammar placeholder, not an accepted token. Its exact spelling, the
compiler-provided query input name (`source` above), and remaining clause order are the next D3
grammar checkpoint. That checkpoint may select syntax but cannot reintroduce source/id repetition
or connection configuration.

Typed clauses cover driver-owned relation selection, logical destination target, disposition,
primary or merge keys, cursor, contract/trust, and execution extent. Unknown, repeated,
contradictory, or out-of-order clauses fail with exact source location. Defaults may come only from
the typed project model and are resolved into the manifest; no generic `WITH` map exists.

Named source configuration, driver options, policy, and secret references live once in `cdf.toml`
and are validated by driver option schemas. SQL cannot contain a source name/type, connection URI,
credential, secret reference/value, or source-level option. Destination connection remains selected
by the environment while the statement may declare only its logical target. Companion metadata
files and metadata headers are forbidden in v1.

The chosen form MUST prove:

- deterministic parsing and helpful source locations;
- DataFusion compatibility for the relational query body;
- typed, schema-validated non-relational options rather than stringly TOML-in-SQL;
- no credentials or secret values;
- exact path-derived resource/source identity and explicit logical destination identity;
- canonical formatting/normalization for hashing without changing SQL semantics;
- forward-compatible rejection of unknown CDF clauses.

Exact syntax is a user-visible semantic blocker. No executable parser ticket may choose it by
convention.

## Initial relational language

The first active language SHOULD support only the surface that can lower completely into reviewed
native CDF operators:

- one upstream relation per resource, interpreted only by the path-bound source type;
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

- the resource path resolves one configured project source before SQL parsing; that source's type
  selects an internal driver, and only then does the relation clause resolve through driver-owned
  semantics. SQL names never resolve directly against the source-driver registry.
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
- path-derived source/resource, effective source-configuration, driver, and upstream-relation
  bindings;
- lowered native expression/operator graph and hash;
- output schema, contracts, semantics, destination plan, and lineage;
- pushdown/residual decisions;
- diagnostics and unsupported/excluded constructs;
- template/macro expansion origin if that later capability exists.

Two SQL spellings that normalize to the same semantics MAY share execution identity only if the
canonicalization law is explicit and tested. Authored-origin hashes remain separately inspectable.

## Current-only authoring transition

CDF is net-new/customer zero. The spike-era TOML/YAML project resource front-end, root wildcard
resource mappings, declaration-file locator, and explicit SQL id receive no coexistence period,
migration reader, or compatibility schema. Foundation D replaces them together.

Reusable declarative/compiler structures MAY remain internal lowering types if their authority and
naming fit the new model. Runtime and destination code MUST remain authoring-format agnostic.
`cdf add`/generation MUST write the source configuration and explicit SQL resource paths through
atomic multi-file publication; it cannot emit the retired mapping/declaration shape.

## Explicitness, discovery, and templating

Initial SQL authoring has no general Jinja/template runtime.

- Catalog discovery may enumerate upstream relations through existing source discovery authority.
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

- SQL containing a source/type name, credential, DSN, secret reference, or source-level option
  fails Contract with remediation to the path-bound `[sources.<name>]` configuration.
- Unknown configured source, relation, resource field, function, or semantic reference fails
  compilation before I/O.
- Ambiguous names require qualification; resolution cannot depend on map iteration order.
- Parser/type/lowering errors retain file, line, column, construct, and stable error code.
- Source/destination discovery and execution errors retain adapter provenance.
- DataFusion internal/planning failures are classified through existing error ownership and never
  exposed as unchecked strings.
- Compiler diagnostics and manifest content use the same redaction authority as CLI JSON/human
  output.

## Acceptance scenarios

1. Given a simple one-source SQL resource, compile produces the expected native source/operator/
   contract/destination artifacts without a runtime SQL or authoring-format branch.
2. Given projection/filter/cast/alias expressions, DataFusion analysis and native CDF execution
   agree on Arrow schema, null behavior, values, and errors over property-generated batches.
3. Given an inexact source pushdown, the manifest records it and native residual evaluation
   preserves results.
4. Given a join/window/unknown function, compilation fails at the exact source location and no
   runtime plan or external I/O is produced.
5. Given a path-bound named source configuration, the SQL/manifest contain no resolved secret
   value.
6. Given `sources/warehouse/orders.cdf.sql`, compilation derives exactly `warehouse.orders`; SQL
   cannot override it, and canonical path collisions fail before manifest publication.
7. Given a spike-era project mapping/declarative resource file, current validation rejects the
   retired shape with regeneration guidance and no compatibility reader.
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

## Staged implementation

1. D1 publishes the active manifest before SQL parsing lands.
2. D2 must activate a focused native scalar/cast allowlist and IR version before D3 accepts those
   expressions. That exact allowlist remains a D2 shaping checkpoint, not parser discretion.
3. D0 removed the current Postgres-special-cased merge-dedup policy. The path-derived source model,
   typed base configuration, and selected-environment overlays are governed by
   `.10x/specs/project-source-resource-layout.md`; driver options stay schema-validated.
4. C1/C2 provide canonical semantic references. Exact SQL semantic-annotation token syntax remains
   a focused D3 shaping checkpoint; it cannot create a second reference grammar.
5. SQL target is logical target authority only; environment configuration owns destination
   connection selection.

## References

- `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md`
- `.10x/specs/project-compilation-manifest.md`
- `.10x/specs/project-source-resource-layout.md`
- `.10x/decisions/filesystem-source-resource-and-configuration-authority.md`
- `.10x/specs/semantic-type-registry.md`
- `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md`
- `.10x/decisions/compiled-fused-streaming-operator-graph.md`
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/specs/types-contracts-normalization.md`
- `VISION.md` D-1, D-2, D-9, D-19, D-20
