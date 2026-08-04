Status: active
Created: 2026-08-04
Updated: 2026-08-04
Supersedes: `.10x/decisions/superseded/project-path-tokens-and-upstream-relation-binding.md`
Completes: D3 authoring grammar and binding choices under `.10x/decisions/filesystem-source-resource-and-configuration-authority.md`

# Project path tokens, query-first resources, and explicit upstream binding

## Context

After resource identity and configured-source identity were separated, D3 still required exact
choices for file form, relation binding, structured values, envelope clauses, defaults, merge keys,
semantics, and execution policy. These are user-visible and affect hashes, manifests, diagnostics,
and runtime admission. They cannot be invented by an executor or delegated to DataFusion.

The user ratified the complete D3 handoff on 2026-08-04. It supersedes the earlier mandatory
`CREATE RESOURCE`, path-bound source, split `DISPOSITION MERGE`/`MERGE KEY`, and source-less
`upstream(...)` grammar. Unchanged decisions—strict tokens, no inactive source configuration,
closed driver schemas, native CDF runtime authority, and rejection of unsupported relational
operators—remain in force.

## Decision

### Exact project-name token grammar

Resource namespace, resource file stem, and configured source names MUST each match:

```text
[a-z][a-z0-9_]{0,127}
```

Each is 1 through 128 ASCII bytes, begins with a lowercase ASCII letter, and thereafter contains
only lowercase ASCII letters, digits, or underscores. The compiler strips exactly one terminal
`.cdf.sql` before validating a resource stem. It preserves accepted tokens byte-for-byte and MUST
NOT case-fold, transliterate, normalize Unicode, substitute punctuation, reuse destination name
normalization, or accept quoted filesystem identities. Invalid tokens fail at their exact authored
path/config location.

### Query-first file grammar

A resource file contains exactly one of:

```text
resource_file := select_query | resource_definition

resource_definition :=
    RESOURCE
    [target_clause]
    [disposition_clause]
    [cursor_clause]
    [trust_clause]
    [semantics_clause]
    [execution_clause]
    AS select_query
```

`SELECT` alone is the normal form. `RESOURCE` is an optional metadata envelope and carries no
identifier. Canonical envelope order is exactly:

```text
RESOURCE
TARGET
DISPOSITION
CURSOR
TRUST
SEMANTICS
EXECUTION
AS
```

Unknown, repeated, contradictory, or out-of-order clauses fail with exact source spans. D3 has no
`CREATE RESOURCE`, identifier after `RESOURCE`, `FROM SOURCE`, `SINK`, generic top-level `WITH`,
generic `OPTIONS`, metadata header, or companion per-resource metadata file.

### Required explicit configured-source binding

The one admitted base relation is:

```sql
upstream(source => '<configured_source>', <driver-owned arguments>...)
```

`source` is a reserved CDF relation argument, required exactly once, named rather than positional,
and a string-literal configured-source name. It is not passed to the driver resource schema. CDF
resolves it against the selected project's typed `[sources.<name>]`, selects that source's immutable
driver type, validates the effective source configuration, removes `source`, and validates all
remaining arguments through the selected driver's closed resource schema.

Missing, repeated, positional, non-literal, or unknown `source` fails at the relation before
external I/O. A source type, driver id, URI, credential, secret reference/value, source-level
option, egress policy, catalog credential, or environment endpoint in SQL also fails. The resource
path namespace is never consulted to infer or validate the configured source name.

### Data-only structured relation arguments

Top-level driver arguments use only `identifier => structured_value` and MUST be named. Their value
grammar is recursively data-only:

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

Objects are typed named values, not JSON strings. Argument and object-member order is nonsemantic;
canonical typed identity sorts/maps according to the closed schema while authored SQL identity
retains exact bytes/order. Duplicate names fail rather than choose a winner.

Column references, arbitrary identifiers as values, functions, arithmetic or Boolean expressions,
casts, subqueries, interpolation, environment references, secret references, JSON escape hatches,
and any executable expression are rejected. Values lower through the ordinary driver resource-
option boundary; no second connector grammar or option authority is created.

### Typed metadata clauses and defaults

The focused clause forms are:

```sql
TARGET logical.target
DISPOSITION APPEND
DISPOSITION REPLACE
DISPOSITION MERGE(key_column, ...)
CURSOR output_column
TRUST EXPERIMENTAL
TRUST GOVERNED
SEMANTICS (output_column => 'canonical.semantic@version(parameters)', ...)
EXECUTION BOUNDED
EXECUTION DRAIN (...)
```

Omission of trust, disposition, or execution is resolved before native plan or manifest publication
with strict precedence:

1. explicit authored clause;
2. applicable typed project resource default;
3. a narrow built-in default;
4. compile failure.

Runtime, ambient environment, destination introspection, and map iteration never supply defaults.
Each effective value records its origin as `authored`, `project_default`, `built_in_default`, or
`resource_path_default`, plus canonical identity and authored span when present.

`TARGET` is deliberately narrower: explicit authored target wins, otherwise it defaults exactly to
the canonical path-derived resource id with `resource_path_default` origin. There is no project
target default. The target is logical; environment configuration still chooses the physical
destination. Cursor and semantic bindings are present only when authored and otherwise resolve to
absence, not a guessed value.

When `TRUST` is omitted and no project default applies, it defaults to `EXPERIMENTAL`. `GOVERNED`
is never implicit. The two trust values are a closed set and retain the existing compiler,
contract, deployment, publication, and observability consequences; D3 does not reduce them to
labels.

The typed project default table is the existing `[defaults]` model. D3 admits only
`experimental|governed` trust, `append|replace` disposition, and complete bounded/drain execution
there. Keyed merge remains explicit because a project-wide `merge` value without resource-specific
keys is incomplete; `cdc_apply` is outside D3.

When `DISPOSITION` is omitted, built-in `REPLACE` applies only when the compiler proves the input is
bounded and replayable and the destination capability admits the operation. Incremental or
unbounded input requires an explicit disposition or an applicable typed project default; otherwise
compilation fails. CDF never silently selects `APPEND`.

`DISPOSITION MERGE(keys...)` requires a nonempty, unique ordered key list. Every key resolves
exactly once against the final output schema and the destination must truthfully support merge.
Native package authority defines null-key handling and duplicate-source-effect reduction. Merge
updates existing keys and inserts missing keys; it does not delete target rows absent from the
input. First-class captured deletes remain separately governed package effects and are applicable
only to merge/CDC policies.

`CURSOR` resolves against exactly one final output column and does not change relation identity.

### Semantic annotations

Any resource with semantic annotations uses the expanded envelope:

```sql
SEMANTICS (
  amount => 'finance.currency@1(code="USD")',
  email => 'cdf.pii@1(class="email")'
)
```

Each left side names exactly one final output field. Each right side is a string literal containing
one canonical semantic reference under `.10x/specs/semantic-type-registry.md`. Compilation fails
on duplicate or unknown fields, malformed references, unavailable definition/version/hash,
Arrow-incompatible semantics, or annotation of protected control fields. Semantic annotation
never changes physical representation; SQL casts remain the physical type-changing operation.
Resolved definition, version, hash, normalized parameters, validation/redaction/mapping effects,
and field binding enter lock/manifest authority.

### Execution policy

Bounded execution is:

```sql
EXECUTION BOUNDED
```

The D3 drain form is purpose-built rather than a generic map:

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

A bounded source may omit the clause only when boundedness is provable from the relation or an
applicable typed project policy. An unbounded source requires a complete explicit or typed-project
drain policy. Missing, contradictory, inapplicable, or incomplete drain policy fails compilation.
Resident supervision is not available through D3.

### Relational admission boundary

D3 admits exactly one `upstream(...)` relation, projection, aliases, deterministic D2 scalar
expressions, Arrow-compatible casts, Boolean `WHERE`, semantic annotations, source pushdown with
recorded residuals, and typed target/disposition/cursor/trust/contract/execution metadata.

D3 rejects joins, cross-resource references, aggregates/grouping, windows, all set operations
including `UNION ALL`, recursive queries, subqueries, nondeterministic or D2-inadmissible
functions, DDL/DML, arbitrary source-native SQL, arbitrary table functions/UDFs, runtime discovery
expansion, and row-level Python/WASM. A known output type is necessary but not sufficient for D2
scalar admission.

General joins are rejected because CDF has no ratified multi-input checkpoint consistency,
shuffle/partition/spill/memory, skew, failure-recovery, or replay contract. Static lookup joins are
also excluded until a dedicated bounded-lookup capability is designed. `UNION ALL` is a future
candidate, not D3: the AST and manifest may support multiple source nodes later, but D3 compiles
exactly one and checkpoints it alone.

### Identity law

The compiler records two distinct identities:

- **authored identity** — exact SQL bytes/hash, bare-versus-envelope form, authored spans/order, and
  normalized authored AST hash;
- **effective execution identity** — all resolved metadata, canonical typed source/config/argument
  identity, native scalar/relational plan, policies, and compiler dependency tuple.

A bare query and an explicit envelope may share effective execution identity only when every
effective value and relevant canonical policy is equal. Their authored hashes remain distinct.
Stable source-node identity derives from resource id, configured source, canonical typed arguments,
and logical query-node identity—not source-node ordinal alone.

## Alternatives considered

### Require a declaration keyword in every file

Rejected. The file path already declares the resource. Mandatory ceremony makes the common case
no clearer and duplicates information supplied by typed defaults.

### Keep `CREATE RESOURCE`

Rejected. CDF is not issuing catalog DDL. `RESOURCE` is an optional metadata envelope, not an
object-creation statement.

### Keep split `MERGE KEY`

Rejected. Keys are intrinsic to the merge disposition; separating them permits contradictory and
order-dependent states.

### Allow arbitrary SQL functions when DataFusion finds a type

Rejected. Output type alone does not prove determinism, representability, ambient independence, or
reproducible vectorized execution under native CDF authority.

### Admit joins or `UNION ALL` now

Rejected. Explicit source binding removes the architectural coupling that would block future
multi-source work, but admitting operators before checkpoint/package/memory/replay semantics exist
would create leaky partial support.

## Consequences

- The smallest resource is a readable query with one explicit logical dependency.
- Effective defaults are typed, deterministic, observable, and hashable; no behavior remains
  implicit at runtime.
- Driver resource schemas remain the only connector-specific relation-argument authority.
- Semantic types, trust, merge keys, and drain behavior receive exact compile-time diagnostics and
  durable manifest evidence.
- Future multi-source work can extend an AST that already models source nodes explicitly without
  changing resource identity, but no such execution promise exists in D3.
- D3 must reject every retired syntax with focused current-form guidance, not parse it through a
  shim.
