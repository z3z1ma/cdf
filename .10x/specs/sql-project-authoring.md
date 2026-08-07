Status: active
Created: 2026-08-03
Updated: 2026-08-07
Supersedes: `.10x/specs/superseded/sql-project-authoring.md`

# Query-first SQL project authoring and native CDF lowering

## Purpose

This specification defines D3: the current, query-first `.cdf.sql` authoring language and its
deterministic lowering into native CDF plans. It replaces the mandatory `CREATE RESOURCE`,
path-bound-source, and declarative project-resource front ends.

The SQL surface is a compiler front-end, not a runtime query engine, scheduler, catalog DDL
language, source-native SQL pass-through, or second connector configuration system.

## Non-negotiable architecture

```text
resource path + authored SQL + typed source/destination/default configuration
→ path-derived resource context
→ CDF envelope/query parsing
→ explicit configured-source resolution
→ driver-owned resource-argument validation
→ ephemeral DataFusion analysis
→ CDF schema/contract/semantic/policy validation
→ typed default resolution
→ closed native CDF source/scalar/relational/destination IR
→ canonical project manifest
→ ordinary CDF execution/package/receipt/checkpoint path
```

DataFusion owns query parsing and deterministic scalar analysis where specified below. It MAY exist
ephemerally inside compiler/engine implementation crates. A DataFusion logical/physical plan,
expression debug string, session state, or runtime replanning MUST NOT appear in kernel/runtime/
source public types, native plan identity, manifest, package, receipt, checkpoint, destination
protocol, or replay authority.

Runtime receives a completely resolved native plan. It MUST NOT reparse SQL, re-resolve defaults or
source bindings, repeat semantic analysis, reinterpret the envelope, or choose an alternate plan.
A native batch operator may invoke the exact pinned DataFusion/Arrow scalar implementation under
`.10x/decisions/datafusion-deterministic-scalar-closure.md`.

## Authoring forms

### Bare query: the normal form

```sql
SELECT *
FROM upstream(
  source => 'github',
  glob => 'part-*.snappy.parquet',
  format => 'parquet'
);
```

The file is the declaration. A bare query is syntactic sugar for a resource whose effective target,
disposition, trust, and execution policy have been fully resolved, and whose absent cursor and
semantic bindings have been recorded, before publication.

### Optional typed metadata envelope

```sql
RESOURCE
TARGET warehouse.userdata
DISPOSITION MERGE(user_id)
TRUST GOVERNED
AS
SELECT user_id, email, updated_at
FROM upstream(
  source => 'github',
  glob => 'part-*.snappy.parquet',
  format => 'parquet'
);
```

`RESOURCE` is an envelope marker, not DDL, and takes no identifier. Bare and expanded forms lower
to the same native resource-definition IR after defaults are resolved.

## Independent identities and path authority

For `cdf/analytics/userdata.cdf.sql`:

- canonical authored resource id is `analytics.userdata`;
- `upstream(source => 'github', ...)` selects configured source `github`;
- `TARGET warehouse.userdata` selects logical destination target `warehouse.userdata`.

None is inferred from another. The path derives only namespace, resource name, canonical id, and
default logical target. The resource namespace need not equal the configured source name. SQL
cannot declare the canonical id. Physical destination selection remains environment-owned.

The only current root is `cdf/`; it is a tool-ownership marker excluded from resource identity. The
compiler rejects `sources/`, generic `resources/`, `pipelines/`, wildcard maps, declarative
resource files, explicit SQL ids, and all compatibility forms.

## Grammar

### File and envelope grammar

```text
resource_file := select_query | resource_definition

resource_definition :=
    RESOURCE
    [target_clause]
    [disposition_clause]
    [delete_clause]
    [cursor_clause]
    [trust_clause]
    [semantics_clause]
    [execution_clause]
    AS select_query
```

Canonical clause order is exactly:

```text
RESOURCE
TARGET
DISPOSITION
DELETE
CURSOR
TRUST
SEMANTICS
EXECUTION
AS
```

Every clause appears at most once. Unknown, repeated, contradictory, or out-of-order clauses fail
with exact file/line/column spans and a stable CDF diagnostic code. No parser normalization may
silently reorder authored clauses.

D3 rejects:

- `CREATE RESOURCE`;
- an identifier following `RESOURCE`;
- `FROM SOURCE`;
- `SINK`;
- generic top-level `WITH` or `OPTIONS` maps;
- metadata headers or sidecar per-resource configuration;
- multiple statements or resources in one file.

### Target

```text
target_clause := TARGET logical_target
```

The target uses the existing logical destination identifier rules. When omitted, it resolves
exactly to the canonical path-derived resource id with origin `resource_path_default`. There is no
typed project target default. It never selects a physical destination or connection.

### Disposition

```text
disposition_clause :=
    DISPOSITION APPEND
  | DISPOSITION REPLACE
  | DISPOSITION MERGE '(' output_column (',' output_column)* ')'
  | DISPOSITION CDC_APPLY '(' output_column (',' output_column)* ')'
```

`MERGE` requires at least one key. Keys preserve authored order, are unique, and each resolves
exactly once against the final output schema after projection/aliasing. Empty, duplicate, missing,
ambiguous, or unknown keys fail at their exact token. The destination must truthfully advertise
merge support.

Merge updates existing keys and inserts missing keys. It does not delete target rows absent from
the input. Package-native captured delete effects and explicit hard/soft/ignore application remain
governed by `.10x/specs/package-keyed-delete-effects.md`. Null-key behavior and duplicate input
effect reduction MUST use native package authority; SQL does not create a second rule.

`CDC_APPLY` requires at least one key and one immediately following `DELETE` clause. It consumes
package-native complete upsert/key-only-delete effects and uses protocol-order last-effect
semantics. Source mode, bootstrap, and execution lifecycle are governed by
`.10x/specs/cdc-resource-authoring-and-continuous-run.md`.

### Delete application

```text
delete_clause :=
    DELETE HARD
  | DELETE IGNORE
  | DELETE SOFT '(' output_column ')'
```

`DELETE` is mandatory exactly once for `CDC_APPLY`, and for a deletion-capable `MERGE`; it is
forbidden when deletes cannot enter that merge and under `APPEND` or `REPLACE`. There is no
default. `SOFT` resolves one non-null Boolean marker under the package keyed-effect contract; it
does not invent a timestamp or sparse tombstone.

When disposition is omitted, precedence is explicit clause, applicable typed `[defaults]`
`write_disposition`, narrow built-in default, then compile failure. Project defaults admit only
`append` or capability-safe `replace`; keyed merge remains explicit because a keyless project
default is incomplete. Built-in `REPLACE` is admitted only when the compiler proves the source is
bounded and replayable and the selected destination supports it. Incremental or unbounded input
without an applicable explicit/project disposition fails. The compiler never silently chooses
`APPEND`, `MERGE`, or `CDC_APPLY`.

### Cursor

```text
cursor_clause := CURSOR output_column
```

The cursor resolves to exactly one final output column and records exact Arrow type/nullability and
origin. A missing/ambiguous column or incompatible source capability fails at the clause. Cursor
metadata cannot override the source's truthful position semantics.

### Trust

```text
trust_clause := TRUST EXPERIMENTAL | TRUST GOVERNED
```

The D3 authoring and typed `[defaults].trust` set is closed to these two values. Omission resolves
through an applicable typed project default, otherwise built-in `EXPERIMENTAL`; `GOVERNED` is
never implicit. Older `financial`/`serving` project-default spellings receive no D3 compatibility
path; the underlying kernel presets remain separate non-D3 authority.

The consequences are concrete:

- `EXPERIMENTAL` lowers to the existing experimental contract preset: evolve schema, variant
  capture for nested unknowns, sampled profiling, quarantine disabled, ephemeral retention, and
  fail-on-violation behavior. Compile/manifest publication records the effective trust and origin;
  it makes no governed deployment or promotion claim.
- `GOVERNED` lowers to the existing governed contract preset: evolved columns require the governed
  review artifact, row validation is full, quarantine is enabled, and packages are retained. A
  missing required governed contract/review artifact blocks plan publication or execution under
  the existing contract authority. Manifest, plan/explain, run/status, package evidence, and trust
  ledger surfaces all expose the governed level and resulting policy hashes.

D3 does not invent an independent deployment engine or trust lifecycle. Promotion/demotion,
schema promotion/review, publication safety, status, and runtime enforcement remain governed by
the existing schema/contract/trust records; D3's obligation is exact preset binding, prerequisite validation,
and complete manifest/observability evidence.

### Semantic annotations

```text
semantics_clause :=
    SEMANTICS '(' semantic_binding (',' semantic_binding)* ')'

semantic_binding := output_column '=>' string_literal
```

Example:

```sql
SEMANTICS (
  amount => 'finance.currency@1(code="USD")',
  email => 'cdf.pii@1(class="email")'
)
```

Each right side contains exactly one canonical semantic reference under
`.10x/specs/semantic-type-registry.md`; this clause creates no alias or second grammar. Each left
side resolves to one final output field. The compiler rejects empty clauses, duplicate fields,
unknown/ambiguous output fields, malformed references, unresolved definition/version/hash,
incompatible Arrow types, or protected CDF control fields.

An annotation changes semantic meaning and downstream validation/redaction/mapping behavior, never
physical representation. `CAST`/`TRY_CAST` remain the only SQL type-changing constructs. The
manifest records the field binding, exact definition/version/hash, normalized parameters, Arrow
compatibility, validation, redaction, and destination mapping refinement. Because a bare query has
no annotation surface, any annotated resource uses the expanded envelope.

### Execution policy

Bounded form:

```sql
EXECUTION BOUNDED
```

Drain form:

```sql
EXECUTION DRAIN (
  CHECKPOINT ROWS 100000,
  PACKAGE BYTES 67108864,
  UNTIL DURATION MILLISECONDS 60000,
  WATERMARK DISABLED,
  LATE DATA QUARANTINE,
  SAFE FRONTIER CANONICAL ADMITTED SOURCE POSITION
)
```

The drain members are purpose-built typed policy, not an open map. Their canonical order, units,
positive/range validation, source-capability applicability, and required/optional status are owned
by the existing stream-policy model. D3 MUST parse this exact vocabulary into that model and reject
unknown, repeated, contradictory, incomplete, zero/overflowing, or inapplicable members at their
tokens.

A bounded source may omit execution only when boundedness is proven from the compiled relation or
an applicable typed `[defaults].execution` policy. An unbounded source requires a complete explicit
or typed project drain policy. The existing `[defaults]` table gains the closed execution
declaration; no generic option map is introduced. If the compiler cannot prove extent or fully
resolve policy, it fails.
Resident supervision is excluded.

## `upstream(...)` relation

### Reserved source argument

Every D3 resource query contains exactly one base relation named `upstream` with exactly one
required reserved argument:

```sql
source => '<configured_source>'
```

`source` must be named and a string literal. It is owned and removed by CDF before driver resource
validation. Resolution is:

1. validate the source-name token;
2. resolve exact `[sources.<name>]` in typed project config;
3. apply the selected environment overlay;
4. select the immutable source type and internal driver;
5. validate effective source options through the driver source schema;
6. validate every remaining relation argument through the driver resource schema.

Missing, positional, duplicate, non-literal, or unknown `source` fails before driver argument
validation, DataFusion planning that could contact a provider, or external I/O. The path namespace
is irrelevant to this lookup.

The SQL file MUST NOT contain a source type, driver id, connection URI, credential, secret value or
reference, source-level configuration, egress policy, catalog credential, or environment endpoint.
Focused diagnostics direct users to `[sources.<name>]` without echoing secret-shaped input.

### Recursive structured values

Remaining top-level arguments use only `identifier => structured_value`:

```text
structured_value :=
    string_literal
  | numeric_literal
  | boolean_literal
  | NULL
  | ARRAY '[' [structured_value (',' structured_value)*] ']'
  | OBJECT '(' [identifier '=>' structured_value
                 (',' identifier '=>' structured_value)*] ')'
```

Values are data, never executable expressions. The compiler rejects column references, arbitrary
identifiers as values, functions, arithmetic/Boolean expressions, casts, subqueries,
interpolation, environment lookups, secret references, JSON escape hatches, and executable tagged
forms. Objects use named members and reject duplicates.

Top-level and object-member order is nonsemantic after closed-schema validation. Equivalent values
in different orders share canonical typed relation identity; authored bytes and normalized authored
AST identity remain separate. Unknown, missing, repeated, wrong-type, source-level, or unsupported
driver arguments fail at exact spans through ordinary driver-schema error ownership.

## Initial relational language

D3 admits only the surface that lowers completely into reviewed native CDF operators:

- exactly one `upstream(...)` base relation;
- explicit projection, aliases, and literals;
- deterministic D2-admitted built-in scalar expressions;
- explicit Arrow-compatible `CAST` and `TRY_CAST`;
- Boolean `WHERE` with DataFusion three-valued semantics;
- output semantic annotations;
- source pushdown negotiation with exact/inexact/unsupported residual recording;
- typed target, disposition/merge keys, cursor, trust/contract, and execution metadata.

D3 rejects before external I/O or native-plan publication:

- joins and cross-resource references;
- all set operations, including `UNION ALL`, `UNION`, `INTERSECT`, and `EXCEPT`;
- aggregation, grouping, windows, recursive queries, and subqueries;
- nondeterministic, stable/volatile, ambient-session, UDF, extension, opaque, or otherwise D2-
  inadmissible functions even if DataFusion knows an output type;
- table functions other than CDF's `upstream(...)` boundary;
- DDL/DML, stored procedures, arbitrary source-native SQL/aggregation pipelines;
- runtime table-discovery expansion and row-level Python/WASM.

General joins require multi-input source positions, consistency, checkpoint alignment,
partition/shuffle/hash/spill/memory/skew policy, failure recovery, and replay semantics that D3 does
not own. Static lookup joins receive no exception. `UNION ALL` remains a future design candidate;
explicit source binding and source-node ASTs leave room for it without making a D3 promise.

### D2 scalar admission

DataFusion parses, resolves overloads, inserts/coerces casts, simplifies, and determines output
types/nullability using the exact pinned compiler and feature tuple. CDF admits only fully resolved
built-in scalar expressions whose functions are `Immutable`, whose exact inputs/output/nullability
are within CDF's canonical Arrow closure, whose behavior needs no uncaptured ambient/session
authority, and whose function/signature/cast graph can be represented and rebound vectorially from
typed native IR.

Function aliases canonicalize through DataFusion resolution. Authored hashes remain distinct.
Implicit casts, explicit `CAST`, and `TRY_CAST` remain different durable nodes with exact failure/
null semantics. Unknown or excluded expressions fail with the authored span and failed admission
gate; there is no manual fallback list or opaque runtime node.

## Compiler-owned models

The D3 compiler SHOULD model at least:

```text
ResourceFile
  authored_form: BareSelect | ResourceEnvelope
  resource_context: ResourceCompilationContext
  envelope: AuthoredResourceEnvelope
  query: AuthoredQuery
  span: SourceSpan

ResourceCompilationContext
  canonical_resource_id
  namespace
  resource_name
  authoritative_path
  default_target

AuthoredResourceEnvelope
  target?
  disposition?
  cursor?
  trust?
  semantics?
  execution?

EffectiveResourceEnvelope
  target: ResolvedValue
  disposition: ResolvedValue
  cursor: ResolvedValue?
  trust: ResolvedValue
  semantics: ResolvedValue
  execution: ResolvedValue

ResolvedValue<T>
  value: T
  origin: authored | project_default | built_in_default | resource_path_default
  canonical_identity
  authored_span?

UpstreamRelation
  configured_source
  effective_source_configuration_identity
  driver_identity
  typed_resource_arguments
  stable_source_node_id
  span
```

These names describe authority, not mandatory Rust type names. The implementation SHOULD reuse
existing typed compiler models where their ownership remains correct and MUST NOT add speculative
single-implementation interfaces.

Stable source-node identity derives from resource id, configured source, canonical typed
arguments, and logical query-node identity, never positional order alone. The AST may become a
collection of source nodes in a future language, but D3 validates exactly one.

## Defaults and identity

Trust, disposition, and execution resolution precedence is:

1. authored clause;
2. applicable typed project resource default;
3. narrow built-in default;
4. compile failure.

Target resolves from an explicit clause or the path-derived resource id only. Cursor and semantics
resolve from authored clauses or absence only. The existing typed `[defaults]` table owns trust,
write disposition, and execution: D3 permits `experimental|governed`, `append|replace`, and complete
bounded/drain values respectively. Applicability is checked against compiled source extent/
capabilities, destination capabilities, schema, and policy. No default is resolved from runtime
state, ambient environment, map order, destination introspection, or configured source name.

The compiler retains:

- exact authored SQL bytes and hash;
- bare/envelope form and normalized authored AST hash;
- effective normalized resource-definition hash;
- every resolved value, origin, policy identity, and authored span;
- parser/DataFusion/Arrow/scalar/compiler/normalizer versions.

Two authored files share execution identity only if every effective field, typed dependency,
native plan, and relevant canonical policy is equal. Authored identity never collapses.

## Source, destination, contracts, and semantics

- One resource lowers through the selected driver's ordinary resource schema to one
  `CompiledSourcePlan`; SQL cannot bypass discovery, egress, health, capability, or type policy.
- Projection/filter requests flow through ordinary queryable-resource pushdown; exact/inexact/
  unsupported decisions and native residuals enter plan and manifest identity.
- Destination target/disposition compiles through ordinary destination sheets and validation. SQL
  cannot issue destination DDL/DML or select connection credentials.
- Every output field has exact Arrow type, nullability, provenance, and optional resolved semantic
  reference before contract compilation.
- SQL casts change Arrow representation; semantic annotations change meaning/validation only.
- Contract and trust rules remain CDF artifacts, not DataFusion constraints.
- Transforms cannot remove, rename, or change compiler-declared CDC operation/key or other
  control-critical fields.

## Manifest contract

Successful compilation records at minimum:

- exact authored SQL bytes/hash, bare/envelope form, and normalized authored AST hash;
- effective normalized resource definition and execution identity;
- parser, DataFusion, Arrow, scalar registry, compiler, and normalizer versions;
- authoritative path, namespace, resource name, canonical resource id, and default target;
- effective target, disposition/key fields, delete application/soft marker, cursor, trust,
  semantics, and execution policy, each with origin, canonical identity, and authored span where
  present;
- configured source, effective typed secret-redacted source-config identity, immutable source type,
  driver id/version/descriptor/schema hashes, canonical structured resource arguments, and stable
  source-node identity;
- exact resolved semantic references/definitions/versions/hashes/parameters and field effects;
- native source/scalar/relational/contract/destination IR and typed hashes;
- output schema and nullability, contracts, field/data/control lineage, and protected fields;
- source pushdown and residual decisions;
- excluded/rejected construct diagnostics and their ownership.

No DataFusion plan or debug representation is durable authority. Manifest details additionally obey
`.10x/specs/project-compilation-manifest.md`.

## Diagnostics and security

The compiler MUST produce focused stable diagnostics for:

- invalid initial form or identifier after the no-identifier `RESOURCE` keyword;
- missing, duplicate, positional, non-literal, or unknown configured `source`;
- source type, URI, credential, secret, environment, or source-level config in SQL;
- positional, duplicate, unknown, missing, wrong-type, or executable driver arguments;
- invalid recursive structured values;
- unknown, repeated, contradictory, or out-of-order envelope clauses;
- unsafe or unresolved disposition/execution defaults;
- empty, duplicate, ambiguous, or unknown merge keys;
- missing, repeated, inapplicable, or invalid keyed-change delete application;
- invalid semantic field/reference/definition/version/hash/type/control binding;
- incomplete or inapplicable drain execution policy;
- joins, all set operations including `UNION ALL`, aggregates, windows, subqueries, unsupported
  functions, and every D2 admission failure.

Every diagnostic retains project-relative file, one-based line/column, exact construct span, stable
error code, and error owner. Contract/admission failures are `Contract`; source/destination
refresh/execution failures retain adapter provenance. DataFusion internal failures pass through the
existing error-ownership boundary rather than leaking opaque planner strings. Human/JSON output,
manifest diagnostics, and excerpts share one redaction authority.

Compilation diagnostics occur before external I/O except explicit refresh behavior separately
authorized by the manifest policy. Secret-shaped values are not echoed even when their presence is
itself invalid.

## Current-only cutover and tooling

D3 updates parser/compiler wiring, project loading, lock/manifest binding, run/preview/plan/
validate/inspect selection, `cdf init`, `cdf add`/generation, fixtures, examples, and docs in one
current model. The implementation contains exactly one project-resource reader and no detection,
migration, alias, feature-flag, or compatibility-test machinery for other models.

Generation writes explicit SQL files. D3 includes no Jinja/template runtime or macro language. A
future macro system requires render-and-pin semantics, canonical output hashes/diffs, and a separate
ratified contract. Runtime interpolation remains forbidden.

## Acceptance scenarios

1. Given `cdf/analytics/userdata.cdf.sql` contains one valid bare `SELECT`, compile derives
   resource id and default target `analytics.userdata`.
2. Given a bare query, every omitted effective metadata value is resolved and recorded before
   native plan publication.
3. Given a bounded replayable source and no disposition override, applicable built-in `REPLACE` is
   resolved deterministically.
4. Given an incremental or unbounded source without an applicable disposition default, compile
   fails rather than selecting `APPEND`.
5. Given no trust clause/project override, effective trust is `EXPERIMENTAL` with built-in origin.
6. Given `RESOURCE ... AS SELECT`, explicit clauses override project and built-in defaults.
7. Given `RESOURCE analytics.userdata`, compile rejects the SQL-declared id.
8. Given omitted `source`, compile fails at `upstream(...)`.
9. Given `source => 'github'`, compile resolves `[sources.github]`, selects its immutable driver,
    and validates only remaining arguments through that driver's resource schema.
10. Given resource namespace and configured-source name differ, compile succeeds.
11. Given equivalent structured relation args in different orders, canonical relation identity is
    equal and authored SQL hashes differ.
12. Given `DISPOSITION MERGE(user_id)`, the key resolves against the final output schema.
13. Given empty, duplicate, ambiguous, or unknown merge keys, compile fails at their locations.
14. Given `DISPOSITION CDC_APPLY(user_id) DELETE HARD`, compile records exact key/delete policy;
    omitting the delete clause fails before external I/O.
15. Given valid `SEMANTICS (...)`, exact field binding, definition, version, parameters, and hash
    are recorded and Arrow compatibility is enforced.
16. Given a join, compile fails before external I/O or native plan publication.
17. Given `UNION ALL`, compile fails because set operations are outside the admitted language.
18. Given equivalent bare and expanded resources, effective execution identity matches only when
    all resolved metadata/policy/dependencies match; authored hashes remain distinct.
19. Given successful compilation, no DataFusion plan appears in any durable public, manifest,
    package, receipt, checkpoint, or destination type.
20. Given any defaulted value, the manifest records effective value and exact origin.
21. Given projection/filter/cast/alias expressions, pinned DataFusion analysis and native CDF
    execution agree on schema, values, nulls, and errors over differential/property fixtures.
22. Given inexact pushdown, manifest records the decision and native residual evaluation preserves
    results.
23. Given a non-immutable, ambient, UDF, table, aggregate, window, opaque, or otherwise inadmissible
    function, compile fails at the expression and publishes no plan.
24. Given SQL attempts to alter a protected CDF operation/key field, compile fails as control-
    critical.
25. Given a complete DRAIN policy, it lowers exactly to native stream policy; an incomplete or
    inapplicable policy fails.
26. Given `TRUST GOVERNED`, the governed contract preset and required review/validation/quarantine/
    retention evidence are compiled and exposed consistently.
27. Given unrelated project directories outside `cdf/`, resource enumeration ignores them.

## Performance requirements

- Measure compilation separately from execution throughput.
- Native execution remains vectorized over Arrow under the existing fused operator/memory path.
- No per-row parser, dynamic-language call, cell materialization, boxed row dispatch, or runtime
  DataFusion semantic planning is permitted.
- D2's differential and roofline gates govern pinned scalar kernels; source/destination direct-
  library/protocol rooflines remain unchanged by authoring syntax.
- Focused D3 validation targets affected crates and compiler fixtures; one affected-boundary
  certificate follows a stable tranche rather than whole-workspace repetition after each edit.

## D3 implementation scope

D3 is authorized to implement:

1. bare `SELECT` resources;
2. optional no-id `RESOURCE ... AS` envelope;
3. removal/rejection of `CREATE RESOURCE`;
4. `cdf/<namespace>/<resource>.cdf.sql` path identity and default target;
5. typed project/built-in defaults with origin;
6. required `source => '<configured_source>'`;
7. source resolution before driver resource-argument validation;
8. recursive data-only structured values;
9. `DISPOSITION MERGE(key, ...)`;
10. `SEMANTICS (...)`;
11. bounded and purpose-built drain execution syntax;
12. native lowering and complete manifest recording;
13. rejection/deletion of every retired authoring surface;
14. continued rejection of joins, set operations, and multiple upstream relations.

D3 is not authorized to implement path-inferred source identity, resource namespace/source
equality, SQL-declared ids, connection details in SQL, joins/static lookup joins, `UNION ALL`,
multiple upstream relations, aggregation/windows/subqueries, arbitrary SQL pass-through, source-
native query text, runtime DataFusion planning, resident supervision, generic option bags, or
templating.

## References

- `.10x/decisions/filesystem-source-resource-and-configuration-authority.md`
- `.10x/decisions/project-path-tokens-and-upstream-relation-binding.md`
- `.10x/specs/project-source-resource-layout.md`
- `.10x/specs/project-compilation-manifest.md`
- `.10x/specs/semantic-type-registry.md`
- `.10x/decisions/datafusion-deterministic-scalar-closure.md`
- `.10x/specs/datafusion-scalar-relational-ir.md`
- `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md`
- `.10x/decisions/compiled-fused-streaming-operator-graph.md`
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/specs/types-contracts-normalization.md`
- `.10x/specs/package-keyed-delete-effects.md`
- `.10x/specs/cdc-resource-authoring-and-continuous-run.md`
- `.10x/knowledge/net-new-no-compatibility-policy.md`
- `VISION.md` D-1, D-2, D-9, D-19, D-20
