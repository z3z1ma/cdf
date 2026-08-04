Status: active
Created: 2026-08-04
Updated: 2026-08-04

# Project source/resource layout and configuration

## Purpose

This specification defines what a CDF project looks like, how a source shares configuration across
many resources, how environments refine that configuration, and how filesystem paths become
canonical source/resource identity. It governs the project compiler, SQL authoring, manifest,
lockfile, scaffold, add/generate commands, validation, and inspection surfaces.

It is governed by
`.10x/decisions/filesystem-source-resource-and-configuration-authority.md` and the net-new/no-
compatibility policy.

## Canonical project tree

A source-resource project has this shape:

```text
<project>/
├── cdf.toml
├── cdf.lock
├── sources/
│   ├── warehouse/
│   │   ├── orders.cdf.sql
│   │   └── customers.cdf.sql
│   └── salesforce/
│       ├── accounts.cdf.sql
│       └── opportunities.cdf.sql
├── semantics/
│   └── currency.toml
└── .cdf/
    └── manifest.json
```

Only `cdf.toml`, `cdf.lock`, `sources/`, and existing compiler-required authored artifacts are
required by this spec. `semantics/` exists when project-defined semantics are authored. `.cdf/` is
generated local state and is not source authority. The illustrated `semantics/currency.toml` makes
the desired project-defined-type placement visible but does not ratify the exact C2 definition path
or file grammar.

## Vocabulary and identity

Project source-directory and resource-file-stem tokens MUST match
`[a-z][a-z0-9_]{0,127}` exactly. They are preserved byte-for-byte; the compiler performs no case,
Unicode, punctuation, or destination-name normalization. These filesystem identities are distinct
from the broader kernel `ResourceId`, internal `SourceDriverId`, and destination identifier policy.

### Source type

A source type is a canonical token resolved through the internal `SourceRegistry`, such as
`postgres`, `rest`, `files`, or `mongodb`. The selected driver owns its closed source/resource option
schemas and relation semantics.

### Source

A source is one named configured upstream instance. Its name comes only from the immediate
directory below `sources/` and MUST exactly match one `[sources.<name>]` entry in `cdf.toml`.

### Relation

A relation is the upstream object or selector declared inside one resource SQL file and interpreted
by the source's driver: database table, collection, REST path, catalog table, file selector, or an
equivalent typed driver-owned identity.

### Resource

A resource is one `.cdf.sql` file. For `sources/<source>/<resource>.cdf.sql`, its canonical id is
`<source>.<resource>`. This identity governs lock entries, manifest rows, packages, checkpoint
scope, status, CLI selection, destination planning, and lineage.

The path-derived source/resource id MUST be preserved verbatim after canonical identifier
validation. The compiler MUST NOT derive a second id from SQL text, map patterns, source relation
names, destination targets, or driver output.

## `cdf.toml` source model

Root project configuration MUST contain a closed source map:

```toml
[sources.warehouse]
type = "postgres"
connection = "secret://env/WAREHOUSE_DSN"
database = "analytics"

[sources.salesforce]
type = "salesforce"
connection = "secret://env/SALESFORCE_TOKEN"

[environments.prod.sources.warehouse]
connection = "secret://vault/prod/warehouse"
database = "analytics_prod"
```

Each base source MUST contain exactly one `type`. Remaining keys are source-level options validated
by the selected driver's closed source option schema. Unknown fields fail before external I/O.

Environment source entries are sparse overlays:

- they may override only driver-admitted source option values;
- they inherit unspecified source options from the base source;
- they MUST NOT add an undeclared source, remove a source, rename a source, or override `type`;
- the effective selected-environment source configuration MUST validate as a complete source
  configuration before any resource is lowered;
- overlay ordering is base, then exactly the selected environment; it never depends on map order or
  ambient files.

Secret-bearing fields MUST contain `secret://provider/key` references. Resolved secret values are
runtime-only and MUST NOT enter SQL, `cdf.toml` publication diagnostics, lockfiles, manifests,
compiled plan display, or hashes except under an existing explicit redacted identity law.

CDF MUST NOT interpret `${...}`, shell syntax, Jinja, or arbitrary environment-variable expansion
inside source configuration. `secret://env/NAME` is resolved by the secret provider; ordinary
non-secret environmental differences use typed overlays.

## Filesystem discovery

The compiler MUST enumerate only regular `*.cdf.sql` files exactly two components below the
`sources/` root. It MUST apply the established project-root path fence and MUST reject:

- symlink traversal or a path escaping the project root;
- nested resource directories in v1;
- files directly under `sources/`;
- source directories with no matching source configuration;
- source configurations with no matching source directory;
- configured source directories containing no valid regular `<resource>.cdf.sql` file;
- duplicate/colliding canonical identifiers;
- invalid source directory or resource file-stem identifiers;
- non-UTF-8 or otherwise unhashable authored input paths where the project path contract forbids
  them.

Every accepted source and resource token MUST satisfy the exact grammar in
`.10x/decisions/project-path-tokens-and-upstream-relation-binding.md`. Invalid tokens fail rather
than normalize. Unrecognized files MAY be ignored only when their behavior is deterministic and
documented; another `.cdf.sql` suffix or malformed near-match MUST produce a useful diagnostic
rather than silently disappear.

Enumeration order MUST be canonical source name then canonical resource name. Filesystem directory
iteration order is never semantic.

## Resource file contract

Each resource file MUST contain exactly one CDF-owned resource statement and exactly one admitted
relational query body. It MUST NOT contain:

- an explicit canonical resource id;
- a source name or source type;
- a connection URI, credential, or secret value/reference;
- source-level driver configuration;
- a generic option map or metadata header;
- multiple resources, external DDL/DML, or runtime template expansion.

The query body MUST contain exactly one base relation of the form `upstream(name => value, ...)`.
There is no separate relation clause or compiler-provided `source`/`input` table. The selected
driver's closed resource option schema defines the `upstream(...)` signature; arguments are
resource-level typed configuration values, never source configuration or a generic option bag.
The compiler supplies the selected source and canonical resource id from the path before resolving
that relation, as specified by `.10x/specs/sql-project-authoring.md`.

## Compilation algorithm

For one selected environment, the compiler MUST:

1. load and validate root project metadata and environment selection;
2. enumerate and canonically sort source/resource paths under the path fence;
3. join each source directory to exactly one base source configuration;
4. apply the selected environment's sparse source-option overlay;
5. resolve source `type` through the internal driver registry;
6. validate effective source options through that driver without external I/O;
7. parse each resource envelope and relational body with exact source locations;
8. locate exactly one `upstream(...)`, validate its named typed arguments against the already
   selected driver's resource schema, and lower its canonical values to ordinary resource options;
9. lower to one ordinary `CompiledSourcePlan` and native CDF resource plan;
10. validate lock expectations and publish one selected-environment manifest.

The driver registry is consulted only after project source resolution. A source relation cannot
select a driver, bypass the source configuration, or switch source type.

## Environment and identity laws

- Selecting a different environment does not change source/resource canonical ids.
- Different effective source options produce different effective-source/compiled-plan hashes when
  those options are semantic.
- Secret values behind an unchanged secret reference do not enter compilation identity unless an
  existing explicit authority says otherwise.
- Changing source `type`, source directory, resource file name, upstream relation, or resource SQL
  is a semantic project change.
- Moving `orders.cdf.sql` from `warehouse` to `archive` replaces `warehouse.orders` with
  `archive.orders`; there is no automatic state/checkpoint continuity or alias.
- Two byte-identical SQL files at different canonical paths remain different resources while their
  lowered operator fragments MAY share hashes whose typed identity excludes resource identity.

## Manifest and inspection

The project manifest MUST expose enough authority to explain the full resolution chain without
recompilation:

- authored project-relative resource path and input hash;
- derived source/resource names and canonical resource id;
- selected environment;
- source type, driver id/version, descriptor hash, and option-schema hash;
- redacted base source configuration, selected overlay, effective configuration, and their typed
  hashes;
- upstream relation identity and resource-level option hash;
- native compiled source/operator/contract/semantic/destination identities;
- diagnostics for unmatched/orphaned/invalid paths or source configurations.

Human and JSON inspection MUST use the same names. Neither output may call a declaration file path
a `source`, call a driver a configured source, or describe `SourceRegistry` as a project catalog.

## Publication and tooling

- `cdf init` MUST create this project layout and never scaffold a wildcard resource map or
  declarative `resources/<source>.toml` file.
- `cdf add`/generation MUST plan an explicit `[sources.<source>]` change when the source does not
  exist and one or more `sources/<source>/<resource>.cdf.sql` files.
- Multi-file changes involving `cdf.toml`, SQL files, lock, and manifest MUST use the existing
  crash-safe project publication contract with `cdf.lock` as the final commit point when it changes.
- Dry-run commands MUST render the proposed canonical paths, identities, source configuration
  changes, and manifest/lock effects without writing.
- File watchers and read-only commands MUST treat authored-input changes as stale manifest data;
  they do not silently compile or recover.
- A project with no configured sources/resources MAY be empty. Once `[sources.<source>]` exists,
  it MUST be published atomically with at least one valid resource; no inactive source state exists.

## Error behavior

- invalid/missing source config, invalid path identity, duplicate resource id, invalid relation, or
  unknown source type/option: `Contract` with the exact path/key and remediation;
- manifest/path/config hash mismatch after publication: `Data` corruption/staleness under the
  manifest spec;
- source driver changes compiler-owned source/resource identity: `Contract` at the driver boundary;
- inconsistent compiler cross-reference after validated inputs: `Internal`;
- permission/capacity/filesystem failure: `Environment`;
- source authentication/contact failure occurs only in explicit refresh/health/execution and keeps
  adapter provenance.

## Acceptance scenarios

1. Given `sources/warehouse/orders.cdf.sql` and `[sources.warehouse]`, compile creates exactly
   `warehouse.orders` without an id or source name inside SQL.
2. Given two SQL resources under `sources/warehouse/`, both inherit one validated effective
   warehouse configuration and retain distinct resource options/plans.
3. Given a production overlay changes the warehouse database and secret reference, production
   compilation records the redacted effective configuration while resource identity remains
   `warehouse.orders`.
4. Given a source directory has no `cdf.toml` entry, compilation fails before SQL lowering or
   external I/O and names `[sources.<name>]` as the fix.
5. Given a source entry has no directory or valid resource file, compilation fails `Contract` and
   requires removing the entry or atomically adding its first explicit resource; no inactive state
   is accepted.
6. Given SQL attempts to declare `warehouse.orders`, `SOURCE warehouse`, a DSN, or a generic source
   option, compilation fails at the exact syntax location.
7. Given a resource file is renamed from `orders.cdf.sql` to `purchases.cdf.sql`, manifest diff
   reports removal of `warehouse.orders` and addition of `warehouse.purchases`.
8. Given filesystem enumeration order changes, manifest bytes and hashes remain identical.
9. Given a driver is registered for type `postgres`, the project source resolves it only through
   `[sources.warehouse].type`; relation text cannot select another driver.
10. Given a spike-era `[resources."warehouse.*"]` mapping or `resources/warehouse.toml`, current
    project validation rejects the retired shape with regeneration guidance and no compatibility
    reader.
11. Given `sources/Warehouse/orders.cdf.sql`, `sources/warehouse/order-items.cdf.sql`, or a token
    longer than 128 bytes, compilation fails with the exact path and
    `[a-z][a-z0-9_]{0,127}` requirement; it never normalizes the path.
12. Given a path-bound Postgres resource uses
    `FROM upstream(table => 'public.orders')`, the driver receives canonical resource option
    `table = "public.orders"`; a positional, unknown, duplicate, or source-level argument fails at
    its SQL location.

## Explicit exclusions

- wildcard project resource maps;
- source sidecar configuration files;
- explicit or overridable SQL resource ids;
- source/type names repeated in SQL;
- arbitrary environment interpolation;
- nested or multi-source resources in v1;
- runtime templating or implicit discovery expansion;
- preserving spike-era project/declarative authoring compatibility;
- a disabled/inactive/preconfigured source state;
- any relation form other than the single path-bound `upstream(...)` table function;
- defining the separately governed D2 scalar/operator closure or semantic-annotation value grammar.

## References

- `.10x/decisions/filesystem-source-resource-and-configuration-authority.md`
- `.10x/decisions/project-path-tokens-and-upstream-relation-binding.md`
- `.10x/specs/sql-project-authoring.md`
- `.10x/specs/project-compilation-manifest.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/knowledge/project-file-publication-recovery.md`
- `.10x/knowledge/net-new-no-compatibility-policy.md`
- `.10x/research/2026-08-03-project-compiler-authority-inventory.md`
