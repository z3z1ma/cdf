Status: active
Created: 2026-08-04
Updated: 2026-08-04
Supersedes: `.10x/decisions/superseded/filesystem-source-resource-and-configuration-authority-resources-root.md`

# CDF-owned resource paths and configured-source authority

## Context

CDF needs one unmistakable project root for query-first resource definitions while preserving the
already-ratified separation between a CDF resource, its configured upstream, and its logical
destination target. `sources/` is misleading because it would give “source” three simultaneous
meanings: a filesystem namespace, a `[sources.<name>]` connector configuration, and the reserved
`upstream(source => '<name>')` binding. `resources/` avoids that collision but is generic and
collides conceptually with other tools' resources in colocated repositories.

The user considered `sources/`, `cdf/`, and `pipelines/` on 2026-08-04 and delegated the final
choice. `cdf/` is selected because the root's job is ownership, not data-domain taxonomy. It stays
accurate beside dbt or other tools, does not promise orchestration semantics, and leaves the two
identity-bearing path tokens free to express the logical resource. `pipelines/` is rejected because
one D3 file defines one output relation and bounded computation, not a multi-stage scheduler or
workflow pipeline.

CDF is net new and customer zero. This decision replaces the never-released `resources/` proposal
directly; there is no compatibility reader, dual root, alias, migration, or old-state handling.

`SourceRegistry` remains an internal process registry mapping driver ids, source kinds, and URI
schemes to implementations and their closed option schemas. It is not a project namespace or SQL
catalog.

## Decision

### Independent identities

CDF uses these terms exactly:

- **CDF root** — the literal project-layout marker `cdf/`; it identifies files owned by CDF and is
  excluded from resource identity;
- **resource namespace** — the first path component below `cdf/`; it organizes CDF resources and
  does not select a configured source;
- **resource** — one path-authored CDF integration unit compiled, packaged, checkpointed, and
  delivered independently;
- **source type** — a connector type such as `postgres`, `rest`, `files`, or `mongodb` selecting one
  internal driver implementation;
- **configured source** — one project-named upstream instance with shared typed connection,
  secret-reference, policy, egress, quota, and driver configuration;
- **upstream relation** — one driver-owned table, collection, API selector, catalog object, or file
  selection read by a resource;
- **logical target** — the destination-side name compiled for the resource, independent of the
  environment-selected physical destination connection.

For `cdf/analytics/userdata.cdf.sql`, the canonical resource id and path-derived default logical
target are both `analytics.userdata`. Its query may read
`upstream(source => 'github', ...)` and explicitly target `warehouse.userdata`. The CDF root,
resource id, configured source, and logical target are separate authorities and MUST NOT be
inferred from one another.

### Filesystem resource authority

Current SQL-authored resources live only at:

```text
cdf/<namespace>/<resource>.cdf.sql
```

The compiler derives exactly:

- resource namespace from `<namespace>`;
- resource name from `<resource>`;
- canonical resource id `<namespace>.<resource>`; and
- default logical target `<namespace>.<resource>`.

The literal `cdf/` root is an ownership marker and contributes no bytes to resource identity. The
path never selects the configured source. SQL MUST NOT declare or override its canonical resource
id. Renaming either identity token is a semantic identity change. Duplicate/colliding identities
fail before publication.

Only `cdf/` is compiler input for project resources. Every other project directory is outside that
input surface and is ignored by resource enumeration. The compiler MUST NOT scan or reinterpret an
unrelated directory as a resource tree.

### Shared configured-source authority

Root `cdf.toml` owns the closed map of configured sources. Every base `[sources.<name>]` contains
exactly one immutable `type`; the selected environment may override only driver-admitted source
options and cannot add, remove, rename, or retype a source. Secret-bearing values remain typed
`secret://provider/key` references. Arbitrary shell, environment, Jinja, or runtime interpolation
is forbidden.

Every accepted query binds exactly one configured source through the required reserved relation
argument `source => '<configured_source>'`. CDF resolves and removes that argument before the
selected driver's closed resource-argument schema validates the remaining data-only arguments.
SQL MUST NOT contain a source type, driver id, connection URI, credential, secret, source-level
configuration, egress policy, or environment endpoint.

Every configured source MUST be referenced by at least one accepted resource in the selected
project, but no configured source requires or implies a same-named resource namespace. An empty
project may contain neither. `cdf add` publishes a source and its first referencing resource in one
crash-safe project-file transaction.

### Resource, compiler, and runtime authority

The `.cdf.sql` file is the resource declaration. The normal form is one admitted bare `SELECT`; an
optional no-identifier `RESOURCE ... AS SELECT` envelope carries typed resource metadata. It owns
the upstream relation, projection/filter/scalars/casts, logical target, disposition/merge keys,
cursor, trust, semantic annotations, execution policy, and contract effects. It owns no physical
connection, destination connection, canonical resource id, generic option bag, or executable
configuration expression.

Compilation resolves in this exact order:

```text
cdf-relative resource path
→ canonical resource id and default target
→ resource envelope and SELECT syntax
→ required upstream(source => '<configured_source>', ...)
→ exact configured source and immutable source type
→ internal SourceRegistry driver
→ closed source and resource configuration validation
→ ephemeral DataFusion query analysis
→ CDF schema, contract, semantic, policy, and default resolution
→ closed native CDF source/scalar/relational/destination IR
→ canonical project manifest
```

Runtime receives the complete native plan and MUST NOT reparse SQL, resolve paths/defaults/source
names, or invoke DataFusion planning.

The selected-environment manifest records the authoritative `cdf/` path, path-derived identity,
authored identity, effective execution identity, logical target, configured source, redacted
effective configuration identity, driver, typed arguments, resolved metadata origins, native IR,
schema, semantics, contracts, lineage, and pushdown/residual decisions. Physical destination
selection remains environment-owned; the lock remains dependency/expectation authority.

### Current-only cutover

Foundation D deletes and rejects all non-current authoring shapes together:

- no root wildcard resource map or declarative resource file;
- no `sources/` path-bound-source tree;
- no generic `resources/` root;
- no `pipelines/` alias;
- no SQL-declared resource id or `CREATE RESOURCE`;
- no `FROM SOURCE`, `SINK`, generic top-level `WITH`, or generic `OPTIONS`;
- no source sidecar, compatibility reader, alias, shim, migration mode, or dual publication.

`cdf init`, `cdf add`, generation, validation, compilation, lock/manifest publication, inspection,
examples, and documentation all emit and consume only the `cdf/` layout.

## Alternatives considered

### `resources/`

Rejected. It is semantically broad and can collide with another tool's project files. The
tool-owned root communicates authority more directly.

### `sources/`

Rejected. It visually couples resource namespace to configured-source identity and fights the
explicit `upstream(source => ...)` architecture.

### `pipelines/`

Rejected. It suggests scheduling, branching, and multi-stage orchestration that D3 deliberately
does not expose.

### `datasets/`

Steelmanned as a clear framework-neutral noun for tabular outputs. Rejected because CDF may express
streams, collections, and other Arrow-shaped integration resources; `cdf/` is stable as those
surfaces evolve.

### Repeat source configuration in each SQL file or infer it from the namespace

Rejected. Both duplicate or hide connection authority and collapse identities that must remain
independent.

## Consequences

- Colocated projects make ownership obvious: dbt files remain under dbt roots and CDF files under
  `cdf/`.
- `cdf/analytics/userdata.cdf.sql` stays unambiguous even when it reads configured source `github`
  and targets `warehouse.userdata`.
- The root can survive future relational/streaming evolution without overpromising pipeline
  orchestration.
- D3 changes the existing inventory's root interpretation but preserves its path fencing, exact
  token grammar, stable reads, bounds, deterministic enumeration, and no-inactive-source law.
- Existing spike/prototype source, resource, CLI, scaffold, lock, and manifest code is deliberate
  drift only until the one current-only D3 cutover.
