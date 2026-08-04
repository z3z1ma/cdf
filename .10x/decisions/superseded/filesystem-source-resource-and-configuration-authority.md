Status: superseded
Created: 2026-08-04
Updated: 2026-08-04
Superseded-By: `.10x/decisions/filesystem-source-resource-and-configuration-authority.md`
Supersedes: `.10x/decisions/superseded/sql-resource-envelope-and-profile-boundary.md`

# Filesystem source/resource identity and configuration authority

## Context

CDF's spike-era project model uses `source` for four different things:

- an internal Rust driver implementation and its registered kinds/schemes;
- a user-named configured upstream instance;
- a declarative-file or direct-URI locator in `ProjectResource.source`;
- the source half of the compiled `<source>.<resource>` identity.

The current Postgres example demonstrates the resulting duplication: root `cdf.toml` maps
`[resources."warehouse.*"]` to `resources/warehouse.toml`; that file declares
`[source.warehouse]` and `[resource.orders]`; compilation then concatenates those names into
`warehouse.orders`. The mapping key, file name, source declaration, and compiler all participate in
an identity a user cannot see in one place. The user confirmed that the sandbox configuration looks
and feels wrong and asked Foundation D to cement the project model before later CDC, connector,
configuration, and hook work builds on it.

The previously active SQL decision moved connection configuration out of SQL but required every
SQL statement to repeat an explicit canonical resource id. It therefore retained two competing
identity authorities: the filesystem path users navigate and the id written inside the file.

The phrase `source-driver catalog` in the SQL spec was also imprecise. `SourceRegistry` is an
internal process registry mapping driver ids, accepted source kinds, and URI schemes to stateless
driver implementations and their option schemas. It is not a project namespace and is not where a
SQL relation name should resolve.

CDF is net-new and customer zero. There is no compatibility obligation requiring the spike-era
resource map, declarative authoring shape, explicit SQL id, or a dual reader.

This decision also supersedes only the spike-era mapping/layout statements in
`.10x/decisions/data-onramp-source-identity-preview-disposition.md` and
`.10x/decisions/cdf-init-local-scaffold-defaults.md`. Their append/key, normalization,
preview/run-parity, safe overwrite, artifact-preservation, and no-runtime-artifacts rules remain
active.

## Decision

### Ubiquitous language

CDF uses these terms exactly:

- **source type** — a user-facing connector type such as `postgres`, `rest`, `salesforce`, or
  `mongodb`; it selects an implementation through the internal source-driver registry;
- **source** — one project-named, configured upstream instance such as `warehouse` or
  `salesforce`; it owns common typed connection, policy, egress, and driver options;
- **relation** — the driver-owned upstream object selected for one resource, such as
  `public.orders`, `/issues`, or `accounts`;
- **resource** — one CDF-owned integration unit compiled, packaged, checkpointed, and delivered
  independently, with canonical id `<source>.<resource>`;
- **driver** — an internal Rust implementation selected by source type; it is not a project-level
  source name and its registry is not a user-authored catalog.

The same separation applies on the destination side: destination type/implementation, configured
destination, and logical target are distinct concepts, although their exact multi-destination
project shape remains separately governed.

### Filesystem identity

SQL-authored source resources live at exactly:

```text
sources/<source>/<resource>.cdf.sql
```

For the first language version, `<source>` is exactly one directory component and `<resource>` is
exactly one file stem. The path is authoritative:

- directory `sources/warehouse/` names source `warehouse`;
- file `orders.cdf.sql` names resource `orders`;
- the canonical resource id is `warehouse.orders`;
- the SQL file MUST NOT repeat either the source name or canonical resource id;
- duplicate canonical paths cannot exist, and canonical-identifier collisions across filesystem
  spellings fail deterministically;
- the compiler MUST reject invalid or noncanonical path identifiers rather than silently minting a
  different identity;
- renaming the source directory or resource file is a semantic identity change, recorded as such in
  manifest/diff output; it is not an alias, redirect, or compatibility migration.

Nested source/resource namespaces, multi-source models, and filename id overrides are excluded from
v1. A future multi-source transformation surface must receive its own explicit namespace rather
than weakening the one-source-resource path law.

### Shared source configuration

Root `cdf.toml` owns the comparatively small set of named source configurations. The project model
has a typed `[sources.<source>]` entry for every source directory. Each entry declares:

- immutable project-level `type`, resolved through the internal driver registry;
- source-level options shared by all resources in that directory;
- secret references, never resolved credential values;
- shared egress, rate/quota, trust, retry, catalog/database, and connection facts where the source
  driver's closed schema admits them.

The exact structural model is:

```toml
[sources.warehouse]
type = "postgres"
connection = "secret://env/WAREHOUSE_DSN"

[environments.prod.sources.warehouse]
connection = "secret://vault/prod/warehouse"
```

Base source configuration is inherited by each environment. An environment may override only
driver-admitted source option values; it MUST NOT change the source name or source type. Source
type is project structural authority because changing it changes relation grammar, capabilities,
position semantics, and compiled identity.

Secrets use the existing `secret://provider/key` authority. CDF does not add arbitrary `${VAR}` or
runtime string interpolation. `secret://env/NAME` is the explicit environment-provider form for
credentials; non-secret environmental differences use typed source option overlays and are
manifested.

`cdf.toml` contains no resource-to-file map, wildcard resource mapping, direct resource URI, or
per-resource declaration. Resource discovery is deterministic filesystem enumeration under the
`sources/` path fence. A source directory without a corresponding configuration is a blocking
compile-time diagnostic, never an implicit source. A source configuration without a current
resource directory is always reported; whether that report is blocking or permits a deliberately
inactive/preconfigured source remains a focused D1.5 checkpoint.

Source sidecar files such as `sources/warehouse/source.toml` are excluded. They would split one
source's environment/configuration authority across files, complicate atomic publication, and add
no capability beyond one typed `cdf.toml` entry per source.

### SQL/resource authority

One `.cdf.sql` file defines exactly one resource. Its CDF-owned envelope declares only facts that
vary per resource:

- the upstream relation/selector interpreted by the selected source type;
- projection, filtering, casts, aliases, and semantic annotations;
- logical destination target, disposition, keys, cursor, trust/contract, and execution policy.

It contains no source name, source type, connection URI, credential, source-level option map,
canonical resource id, or generic `WITH` configuration bag. The exact relation-clause token and
remaining D3 SQL grammar are governed by the SQL authoring spec; this decision fixes ownership, not
that final spelling.

### Resolution and validation

Compilation resolves in this order:

```text
project-relative resource path
→ derived source name and resource id
→ selected environment's named source configuration
→ source type
→ internal SourceRegistry driver
→ driver validation of source-level options
→ driver validation of resource relation/options
→ CompiledSourcePlan
→ native operator/contract/semantic/destination plans
→ project manifest
```

A `FROM`/relation name never resolves directly against `SourceRegistry`. The project source is
resolved first; only its type selects a driver. The existing driver boundary already distinguishes
source-level and resource-level options, and the new project compiler must preserve that split.

### Manifest and lock authority

The selected-environment manifest records, for every source and resource:

- the authoritative project-relative path and its content hash;
- derived source name, resource name, and canonical resource id;
- source type and exact driver descriptor/option-schema hashes;
- canonical secret-redacted effective source configuration and its hash;
- which base fields and environment overrides produced that configuration;
- driver-owned upstream relation/selector identity;
- all ordinary compiled plan, schema, semantic, contract, destination, and lineage facts.

The lockfile pins semantic/dependency expectations; the manifest records the complete resolved
environment-specific compilation. Neither may preserve a hidden wildcard mapping or reconstruct
identity from display SQL.

### Current-only transition

Foundation D replaces the spike-era authoring model outright:

- no `[resources."pattern"]` project mappings;
- no `resources/<source>.toml` declarative project resources;
- no `ProjectResource.source` file-locator semantics in the resulting project model;
- no explicit resource id in `.cdf.sql`;
- no legacy project reader, migration warning, dual authoring mode, alias, or compatibility shim.

Internal typed structures may be reused as compiler IR only when their names and authority are no
longer exposed as the retired project contract. `cdf init`, `cdf add`, examples, validation,
inspection, lock generation, manifest compilation, and docs must transition together.

## Alternatives considered

### Explicit canonical id inside every SQL file

Rejected. The filesystem remains how humans navigate and organize the project, so repeating the id
creates two authorities and allows meaningless path/id disagreement. A path-derived id is explicit
because the path itself is a required, validated compiler input.

### Keep root wildcard resource mappings

Rejected. They add an indirection layer without representing a distinct concept, permit zero/many
matching surprises, and force users to repeat information already present in the source directory.

### Put shared source configuration in every SQL file

Rejected. It duplicates credentials and driver options, makes environment changes touch many
resources, and turns a relational resource language into stringly operational configuration.

### Put `source.toml` beside the SQL files

Rejected. Colocation is attractive, but environment selection, secret resolution, and atomic
project configuration would be split between root and source directories. Source count is expected
to be much smaller than resource count, so one root entry per source remains legible.

### Arbitrary environment-variable interpolation

Rejected. `${VAR}` expansion makes authored bytes insufficient to explain compilation, can leak
values into diagnostics/manifests, and creates stringly behavior. Typed overlays and secret-provider
references express the two legitimate cases directly.

### Resolve SQL names against the source-driver registry

Rejected. Drivers are implementations, not configured upstream instances or query namespaces.
Direct resolution would bypass project configuration and conflate `postgres` with `warehouse`.

## Consequences

- The active SQL authoring spec and project-format spec must use this vocabulary and path law.
- Existing spike-era examples and product code are deliberate source/spec drift until the bounded
  Foundation D implementation replaces them; they are not compatibility authority.
- The source filesystem is a semantic compiler input and must use existing project-root path-fence,
  stable-read, hashing, and crash-safe publication rules.
- One source can naturally own many resources while connection and policy configuration appears
  once.
- Environment-specific compilation changes effective source configuration hashes without changing
  filesystem resource identity.
- Future `cdf add`/discovery generation writes or proposes an explicit source config plus one or
  more path-derived `.cdf.sql` resources; it never reintroduces wildcard maps.
- Exact safe path-token grammar, the exact SQL relation-clause token, and blocking versus explicitly
  inactive handling for a configured source with no directory remain focused D1.5/D3 checkpoints.
  They cannot change the authority allocation decided here.
