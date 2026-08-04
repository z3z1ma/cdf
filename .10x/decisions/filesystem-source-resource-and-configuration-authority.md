Status: active
Created: 2026-08-04
Updated: 2026-08-04
Supersedes: `.10x/decisions/superseded/filesystem-source-resource-and-configuration-authority.md`

# Resource-path identity and configured-source authority

## Context

CDF needs a project model in which users can define many resources against one configured
upstream without repeating connection and policy configuration. The first Foundation D model used
`sources/<source>/<resource>.cdf.sql` as both logical resource identity and upstream-source binding.
That collapsed three independent concepts:

- the CDF resource users author, compile, run, package, checkpoint, and inspect;
- the named configured upstream instance from which one relation reads; and
- the logical destination target to which the resource writes.

It also forced every file to begin with `CREATE RESOURCE` even though the file path already declared
the resource and the common case needed only a query. The user ratified the complete D3 query-first
handoff on 2026-08-04 and explicitly authorized superseding the earlier model. CDF is net new and
customer zero, so there is no compatibility obligation, temporary dual root, legacy reader, or
migration surface to preserve.

`SourceRegistry` remains an internal process registry mapping source-driver ids, accepted kinds,
and URI schemes to implementations and their closed option schemas. It is not a project namespace,
not an authored source catalog, and not where a SQL identifier resolves.

## Decision

### Ubiquitous language and independent identities

CDF uses these terms exactly:

- **source type** — a connector type such as `postgres`, `rest`, `files`, or `mongodb` that selects
  one internal driver implementation;
- **configured source** — one project-named upstream instance such as `github` or `warehouse`, with
  shared typed connection, secret-reference, policy, egress, quota, and driver configuration;
- **upstream relation** — one driver-owned object or selector such as a table, collection, REST
  path, catalog table, or file glob;
- **resource namespace** — the first path component below `resources/`; it organizes CDF resources
  and does not select a configured source;
- **resource** — one path-authored CDF integration unit compiled, packaged, checkpointed, and
  delivered independently;
- **logical target** — the destination-side name compiled for the resource, independent of the
  environment-selected physical destination connection;
- **driver** — an internal Rust implementation selected by a configured source's immutable type.

For `resources/analytics/userdata.cdf.sql`, the canonical resource id is
`analytics.userdata`. A query in that file may read `source => 'github'` and explicitly target
`warehouse.userdata`. Those three names are independent and MUST NOT be inferred from one another.

### Filesystem resource identity

Current SQL-authored resources live only at:

```text
resources/<namespace>/<resource>.cdf.sql
```

The compiler derives exactly:

- resource namespace from `<namespace>`;
- resource name from `<resource>`;
- canonical resource id `<namespace>.<resource>`; and
- default logical target `<namespace>.<resource>`.

The path never selects the upstream source. The SQL file MUST NOT declare or override its canonical
resource id. Renaming either path token is a semantic identity change, not an alias or checkpoint
migration. Duplicate or colliding canonical identities fail before plan publication.

The previously proposed `sources/<name>/<resource>.cdf.sql` root is retired immediately. CDF MUST
NOT implement a temporary mode that reinterprets its first component as a namespace, because that
would create a second current authoring shape contrary to the ratified net-new policy.

### Shared configured-source authority

Root `cdf.toml` owns the closed map of named configured sources:

```toml
[sources.github]
type = "files"
root = "s3://example-bucket/export"
credentials = "secret://env/GITHUB_EXPORT_CREDENTIALS"

[environments.prod.sources.github]
root = "s3://production-bucket/export"
credentials = "secret://vault/prod/github-export"
```

Each base source contains exactly one immutable `type`; all remaining fields are source-level
values validated by that driver's closed source schema. A selected environment may override only
driver-admitted source option values. It cannot add, remove, rename, or change the type of a base
configured source. Secret-bearing values remain `secret://provider/key` references. Arbitrary
`${...}`, shell, Jinja, or runtime string interpolation is forbidden.

Each accepted `upstream(...)` relation names one configured source explicitly through the required
reserved argument `source => '<configured_source>'`. A configured source name is a safe logical
project dependency, not a credential or connection string. SQL still MUST NOT contain a source
type, URI, credential, secret reference/value, source-level option, egress policy, catalog
credential, or environment endpoint.

The previously ratified no-inactive-source law remains: every `[sources.<name>]` entry MUST be
referenced by at least one accepted resource relation in the selected project. It no longer implies
or requires a same-named resource directory. An otherwise empty project may have no configured
sources and no resources. `cdf add`/generation publishes a new source and its first referencing
resource atomically.

### SQL/resource authority

The `.cdf.sql` file itself is the resource declaration. The normal form is one admitted bare
`SELECT`; an optional no-identifier `RESOURCE ... AS SELECT` envelope carries per-resource metadata
only when needed. The file owns:

- one explicit configured-source binding and its driver-owned structured relation arguments;
- projection, filtering, aliases, deterministic scalar expressions, and casts;
- logical target, disposition and merge keys, cursor, trust, semantic annotations, and execution
  policy through typed clauses or typed defaults; and
- contract effects governed by the existing contract/trust authorities.

It owns no connection configuration, physical destination selection, canonical resource id,
generic option bag, or executable configuration expression.

### Resolution and validation order

Compilation resolves in this exact authority order:

```text
project-relative resource path
→ canonical resource id and default target
→ resource envelope and SELECT syntax
→ required upstream(source => '<configured_source>', ...)
→ exact configured source in typed project configuration
→ immutable source type
→ internal SourceRegistry driver
→ closed source-configuration validation
→ driver-owned resource-argument validation
→ DataFusion query-body analysis
→ CDF schema/contract/semantic/policy validation
→ typed default resolution
→ native CDF source/scalar/relational/destination IR
→ project manifest
```

The compiler MUST resolve `source` before validating the remaining relation arguments because the
selected driver owns their schema. SQL never names a driver directly and never bypasses typed
project configuration. Runtime receives a complete native plan and MUST NOT reparse SQL, resolve
defaults or source names, or run DataFusion planning.

### Manifest, lock, and destination authority

The selected-environment manifest records authored identity separately from effective execution
identity. It explains the path-derived resource id, default or explicit logical target, configured
source, effective secret-redacted source-configuration identity, driver identity, canonical typed
relation arguments, resolved metadata with origin, native IR, schema, semantics, contracts,
lineage, and pushdown/residual choices.

The lockfile remains dependency and expectation authority. Physical destination selection remains
environment-owned; a resource's `TARGET` is logical destination identity only.

### Current-only transition

Foundation D replaces all retired authoring shapes together:

- no root wildcard resource maps;
- no declarative `resources/<source>.toml` resource files;
- no `sources/<source>/<resource>.cdf.sql` path-bound-source root;
- no SQL-declared resource id;
- no `CREATE RESOURCE`, `FROM SOURCE`, `SINK`, generic top-level `WITH`, or generic `OPTIONS`;
- no source sidecars, compatibility readers, aliases, shims, migration modes, or dual publication.

Internal typed structures may survive only when their names and authority fit the current compiler
model. `cdf init`, `cdf add`, generation, validation, compile, lock, manifest, inspection,
examples, and documentation transition atomically with D3.

## Alternatives considered

### Bind the source from `sources/<source>/...`

Rejected. It overloads organizational namespace as connection selection, prevents resources from
being grouped by domain or destination purpose, and precludes a clean future multi-source AST.

### Keep `sources/` temporarily but reinterpret it as a namespace

Rejected for this project. Although mechanically possible, it creates two current roots and a
future deletion task with no customer compatibility requirement.

### Repeat source configuration in every SQL file

Rejected. It duplicates credentials and policy, turns environment changes into broad edits, and
recreates stringly configuration inside SQL.

### Infer configured source from resource namespace

Rejected. Namespace and source are independent identities; inference would merely recreate the
retired coupling under a new root name.

### Declare resource id in SQL

Rejected. The file path is already mandatory authored identity. A second declaration creates
conflict and makes moves ambiguous.

### Resolve directly against the driver registry

Rejected. Drivers are implementations, not configured project dependencies. Direct resolution
would bypass source configuration and conflate `postgres` with an instance such as `warehouse`.

## Consequences

- Users can group resources by business or project namespace while reusing any named source.
- A configured source can be shared across many resource namespaces without duplicated config.
- Bare queries become the default authoring experience; metadata appears only where behavior
  differs from resolved defaults.
- Explicit relation binding makes source dependencies visible and leaves the AST structurally
  capable of future multi-source resources, while D3 still admits exactly one relation.
- The D1.5a inventory remains useful as typed source/config/path input machinery, but D3 must change
  its path interpretation and source-binding order before exposing it.
- Existing spike-era source, resource, CLI, scaffold, lock, and manifest code is deliberate drift
  until the one current-only D3 cutover; it carries no compatibility authority.
