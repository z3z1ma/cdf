Status: active
Created: 2026-08-04
Updated: 2026-08-04
Supersedes: `.10x/specs/superseded/project-source-resource-layout.md`

# Project resource/source layout and configuration

## Purpose

This specification defines the current CDF project tree, independent resource/configured-source
identities, shared source configuration, environment refinement, filesystem discovery, and the
compiler resolution boundary. It governs compilation, SQL authoring, manifest/state bindings,
scaffolding, generation, validation, and inspection.

It is governed by
`.10x/decisions/filesystem-source-resource-and-configuration-authority.md`,
`.10x/decisions/project-path-tokens-and-upstream-relation-binding.md`, and the net-new/no-
compatibility policy.

## Canonical project tree

```text
<project>/
├── cdf.toml
├── cdf/
│   ├── analytics/
│   │   ├── userdata.cdf.sql
│   │   └── sessions.cdf.sql
│   └── finance/
│       └── transactions.cdf.sql
├── semantics/
│   └── currency.toml
└── .cdf/
    └── manifest.json
```

`cdf.toml`, `cdf/`, and compiler-required authored artifacts are the current
project surface. `semantics/` exists when project-defined semantic types are authored.
`.cdf/manifest.json`, `.cdf/compiled/`, and `.cdf/schemas/` are derived local compiler/cache
artifacts, not authored or schema authority. The selected environment's state backend owns active
schema versions.

The `cdf/` root marks CDF ownership and contributes no component to resource identity. It is the
only directory enumerated for project resources. Other project directories are unrelated inputs
and MUST NOT be scanned or interpreted as resource declarations.

## Identity model

### Resource identity

A resource is one regular UTF-8 `cdf/<namespace>/<resource>.cdf.sql` file. Namespace and
resource tokens match `[a-z][a-z0-9_]{0,127}` exactly. The canonical id and default logical target
are `<namespace>.<resource>`.

For `cdf/analytics/userdata.cdf.sql`:

- namespace: `analytics`;
- name: `userdata`;
- canonical resource id: `analytics.userdata`;
- default logical target: `analytics.userdata`.

SQL cannot declare or override the canonical resource id. A path rename removes one resource and
adds another; it creates no state/checkpoint continuity or alias.

### Configured-source identity

A configured source is one exact `[sources.<name>]` key in `cdf.toml`. Its name uses the same token
grammar but is independent of resource namespace. Every admitted resource query names its source
through `upstream(source => '<name>', ...)`.

For example, `cdf/finance/transactions.cdf.sql` may bind `source => 'flolake'`. Compilation
MUST NOT require or prefer a configured source named `finance`.

### Logical target identity

`TARGET` is an optional logical destination object. If omitted, it resolves to the canonical
resource id. It neither identifies the configured source nor selects the physical destination
connection. The selected environment retains physical destination authority.

## Typed project configuration

Root `cdf.toml` contains a closed configured-source map and typed resource defaults:

```toml
[sources.github]
type = "files"
root = "s3://example-bucket/github"
credentials = "secret://env/GITHUB_EXPORT_CREDENTIALS"

[sources.flolake]
type = "iceberg"
catalog = "analytics"

[environments.prod.sources.github]
root = "s3://production-bucket/github"
credentials = "secret://vault/prod/github-export"

[defaults]
trust = "experimental"
write_disposition = "replace"
execution = { mode = "bounded" }
```

Each base source MUST contain exactly one immutable `type`. Remaining keys are source-level values
validated by the selected driver's closed source schema. Unknown fields fail before external I/O.

Selected-environment source entries are sparse overlays:

- they may override only driver-admitted source option values;
- they inherit all unspecified base values;
- they cannot add or remove a source, rename it, or change `type`;
- the complete effective source config validates before resource lowering;
- ordering is base followed by exactly the selected overlay and never depends on map order or
  ambient files.

Secret-bearing fields contain only `secret://provider/key` references. Resolved secret values are
runtime-only and MUST NOT enter SQL, manifest, lock, compiled-plan display, diagnostics, or hashes
except under an existing explicit redacted-identity law. CDF does not interpret `${...}`, shell,
Jinja, or arbitrary environment interpolation.

The existing closed `[defaults]` table is typed project-resource default authority. D3 retains
`trust` and `write_disposition`, adds typed `execution`, and removes retired/unrepresentable values
rather than creating a second default table. D3 trust defaults admit only `experimental` or
`governed`; disposition defaults admit `append` or capability-safe `replace`; keyed `merge` remains
an explicit resource clause because a complete merge default cannot exist without resource-specific
keys. Execution uses the closed bounded/drain declaration shape. No project default exists for
resource id, configured source, target, cursor, or semantic bindings. A default MUST prove its
applicability to relation/source/destination capabilities at compile time. Defaults never contain
credentials, driver resource arguments, source selection, or runtime expressions.

Every configured source MUST be referenced by at least one accepted resource in the selected
project. A missing reference is a blocking `Contract` diagnostic; there is no disabled,
preconfigured, or inactive state. This totality check occurs after SQL source binding, not by
matching directory names.

## Filesystem discovery

The compiler enumerates only regular `*.cdf.sql` files exactly two components below `cdf/`.
It applies the established project-root path fence and rejects:

- traversal, symlink escape, or any resource outside the project root;
- nested resource directories in D3;
- files directly below `cdf/`;
- invalid namespace or resource-stem tokens;
- duplicate or colliding canonical ids;
- malformed `.cdf.sql` near-matches that would otherwise disappear silently;
- non-UTF-8 or unstable authored paths forbidden by project publication authority.

Enumeration order is canonical namespace then resource name. Filesystem iteration order is never
semantic. Unrecognized non-resource files may be ignored only by one documented deterministic
rule.

The compiler separately loads and canonically orders `[sources.<name>]`; no path/config join occurs
until each query's `source` argument has been parsed.

## Resource file contract

Each file contains one bare admitted `SELECT` or one no-identifier `RESOURCE ... AS SELECT`
envelope. It contains exactly one `upstream(...)` base relation in D3. The relation has one required
reserved `source` argument and zero or more closed driver-owned structured resource arguments.

The file MUST NOT contain:

- a canonical resource id or a claim that its namespace is its source;
- source type, driver id, connection URI, credentials, secret references/values, or source-level
  driver configuration;
- generic option maps or metadata headers;
- multiple resources or multiple upstream relations;
- external DDL/DML, runtime template expansion, or runtime DataFusion planning.

## Compilation algorithm

For one selected environment, the compiler MUST:

1. load and validate root project metadata, typed defaults, and environment selection;
2. enumerate and canonically sort resource paths under the path fence;
3. derive canonical resource id and default logical target from each path;
4. parse the bare query or optional envelope with exact source spans;
5. locate exactly one `upstream(...)` and extract the required literal `source` argument;
6. resolve the exact `[sources.<name>]` base entry and selected-environment overlay;
7. resolve immutable `type` through the internal `SourceRegistry` and validate effective source
   options without external I/O;
8. validate the remaining recursive data-only named relation arguments through that driver's
   closed resource schema;
9. ask DataFusion to parse/analyze the admitted query body, then lower it through D2 into native CDF
   scalar/relational IR;
10. resolve schema, semantic, contract, pushdown/residual, destination, and policy authority;
11. resolve every omitted metadata value using explicit/project/built-in/failure precedence and
    record its origin;
12. enforce that every configured source is referenced and no resource/source/target identity is
    conflated;
13. validate lock expectations and publish one selected-environment manifest through the existing
    crash-safe transaction.

No step performs external I/O during locked/offline compile. Explicit refresh remains governed by
the manifest compilation policy.

## Identity and hash laws

- Environment selection does not change canonical resource or configured-source names.
- Effective semantic source options change effective-source/compiled-plan hashes where typed
  identity declares them semantic.
- Secret values behind an unchanged reference do not enter compilation identity.
- Source type, configured source binding, resource path, relation args, SQL, explicit metadata,
  or applicable typed default changes are semantic project changes.
- Two byte-identical files at different paths are distinct resources, though native fragments may
  share hashes whose domain excludes resource identity.
- Reordered structured relation arguments share canonical typed relation identity but retain
  distinct authored SQL hashes.
- Bare and expanded forms share effective execution identity only when all resolved metadata and
  applicable policies are equal.

## Manifest and inspection

The manifest and human/JSON inspection expose, without recompilation:

- authored project-relative path, bytes/hash, and bare/envelope form;
- namespace, resource name, canonical id, and default target derivation;
- selected environment and every metadata value with origin;
- configured source name, base/overlay/effective redacted config identities, immutable type,
  driver id/version/schema hashes, and canonical structured relation args;
- source node id, native source/operator/contract/semantic/destination identities, schema,
  lineage, and pushdown/residual decisions;
- diagnostics for invalid paths, missing/unknown/repeated bindings, and unreferenced configured
  sources.

Inspection never calls a resource namespace a source, a driver a configured source, or
`SourceRegistry` a project catalog.

## Publication and tooling

- `cdf init` creates `cdf/` and one explicit query-first resource file.
- `cdf add`/generation plans one explicit source-config change when needed and one or more
  `cdf/<namespace>/<resource>.cdf.sql` files whose queries contain the source binding.
- Config, SQL, and generated-manifest mutations use the existing crash-safe multi-file publication
  contract with the command's declared final target installed last.
- Dry-run renders proposed paths, canonical ids, source binding/config changes, defaults,
  manifest/state effects, and diagnostics without writing.
- Read-only commands treat changed authored inputs as stale manifest data and do not compile,
  publish, or recover.
- All authoring, scaffold, example, and inspection surfaces use this one model. There is no second
  project-resource reader or alternate input root.

## Error behavior

- invalid path/token, duplicate id, malformed resource file, missing/unknown source, unreferenced
  configured source, invalid source/resource option, or unsafe/missing default: `Contract` with
  stable code and exact file/key/span;
- manifest/path/config hash mismatch after publication: `Data` staleness/corruption under the
  manifest spec;
- driver attempts to change compiler-owned resource/source identity: `Contract` at the boundary;
- inconsistent compiler cross-reference after validated inputs: `Internal`;
- permission, capacity, descriptor, or filesystem failure: `Environment`;
- authentication/contact failure occurs only during explicit refresh/health/execution and retains
  adapter provenance.

## Acceptance scenarios

1. Given `cdf/analytics/userdata.cdf.sql` containing a valid bare query, compile derives id
   and default target `analytics.userdata`.
2. Given that file binds `source => 'github'`, compile resolves exactly `[sources.github]` even
   though the namespace is `analytics`.
3. Given two resources in different namespaces bind `github`, both inherit the same selected,
   validated source configuration while retaining distinct identities.
4. Given a production overlay changes source options and secret reference, production compilation
   records the redacted effective configuration while resource identity is unchanged.
5. Given a resource binds an unknown source, compilation fails at the `source` value before driver
   argument validation or external I/O.
6. Given a configured source is referenced by no accepted resource, compilation fails rather than
   accepting inactive configuration.
7. Given a resource is renamed, manifest diff reports one removal and one addition with no implied
   checkpoint continuity.
8. Given filesystem enumeration order changes, manifest semantic bytes and hashes are unchanged.
9. Given relation arguments are reordered, canonical typed argument identity is unchanged while
   authored SQL identity changes.
10. Given unrelated project directories outside `cdf/`, resource enumeration ignores them.
11. Given uppercase, hyphenated, Unicode, leading-digit, or overlength identity tokens, compile
    rejects rather than normalizes.
12. Given `cdf/finance/transactions.cdf.sql` binds `source => 'flolake'`, compile succeeds
    without requiring `[sources.finance]`.

## Explicit exclusions

- path-inferred source identity;
- resource namespace/source equality requirements;
- wildcard project resource maps or source sidecars;
- arbitrary environment interpolation;
- nested resource namespaces in D3;
- multi-source resources, joins, or set operations;
- runtime templating or implicit discovery expansion;
- inactive configured sources;
- any backwards-compatible project reader, alias, shim, or migration mode.

## References

- `.10x/decisions/filesystem-source-resource-and-configuration-authority.md`
- `.10x/decisions/project-path-tokens-and-upstream-relation-binding.md`
- `.10x/specs/sql-project-authoring.md`
- `.10x/specs/project-compilation-manifest.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/knowledge/project-file-publication-recovery.md`
- `.10x/knowledge/net-new-no-compatibility-policy.md`
- `.10x/research/2026-08-03-project-compiler-authority-inventory.md`
