Status: done
Created: 2026-08-03
Updated: 2026-08-03

# Project compiler and manifest authority inventory

## Question

Where do project configuration and compiled authority live today, what can be reused for a
canonical compilation manifest, and which user-visible compiler choices remain before D1-D3 can
execute?

## Sources and methods

Inspected project models, declarative compilation, lock generation/validation, source/destination
composition, CLI project loading, system SQL, scaffold output, and the complete project-file
publication protocol. Applied `.10x/skills/audit-project-file-publication/SKILL.md` and its required
knowledge records to trace mutation guards, durability order, forward recovery, stable-generation
reads, and error ownership.

Focused source authority:

- `crates/cdf-project/src/models.rs`, `lockfile.rs`, `internal.rs`, `project_files.rs`,
  `lock_cas.rs`, `sources.rs`, and `scaffold.rs`;
- `crates/cdf-declarative/src/declarations.rs` and `compiled.rs`;
- `crates/cdf-runtime/src/source.rs`;
- `crates/cdf-cli/src/context.rs`, `project_command.rs`, `sql_command.rs`, and `system_sql.rs`;
- `crates/cdf-cli-core/src/args.rs`;
- `.10x/knowledge/project-file-publication-recovery.md` and
  `.10x/knowledge/content-addressed-sidecar-publication.md`.

## Findings

### Current authority map

| Authority | Current owner | Observed behavior |
|---|---|---|
| project/environment/resource mapping | root `cdf.toml` → `cdf-project::ProjectConfig` | project metadata, environment state/package/destination URI, destination policy, defaults, and resource source mappings |
| source/resource declarations | TOML/YAML files → `cdf-declarative` | source instances, driver options, resource descriptors, schemas, contracts/trust, execution, cursor/key/disposition |
| driver option truth | source/destination registries and driver JSON schemas | compilation validates through concrete driver registrations; generic project code does not own adapter option meaning |
| compiled resource | `cdf-declarative::CompiledResource` | descriptor, schema, capabilities, complete `CompiledSourcePlan`, effective schema runtime, type allowances, execution extent, and project origin |
| expected/pinned facts | root `cdf.lock` → `cdf-project::CdfLock` version 1 | dependency tuple, normalizer, resource descriptor/capability/extent/schema/contract snapshots, destination sheet/protocol capabilities |
| execution-only plan evidence | in-memory plans and later package artifacts | destination commit plans, operator/contract details, and package plan evidence are not available as one pre-run project graph |
| read-only artifact SQL | `cdf sql` → in-memory SQLite | currently mounts checkpoints and package manifests only |

`ProjectContext::load` parses `cdf.toml`, recompiles every declarative resource through the builtin
source registry, and only then loads `cdf.lock`. Therefore current `cdf sql` recompiles project
resources even though its query catalog exposes no project-compilation tables. D1 must not bolt
manifest mounting onto that path and then claim no recompilation; it needs a stable minimal project
location/environment load followed by verified manifest read.

### Existing typed identities available for reuse

`CompiledSourcePlan` already carries canonical schema serialization, driver/version/option schema,
redacted options, physical plan and typed hash, source semantics hash, capability and discovery
bindings, and validation. `CompiledResource`, contract snapshots, schema snapshots, destination
sheets, execution extents, and project origins supply most of D1's resource graph. The manifest
should reference these exact meanings rather than hash display/debug strings or invent parallel
generic JSON authority.

The first manifest can serialize validated typed manifest models in `cdf-project`; it does not need
a new database, service, or runtime planner.

### Publication authority

`.cdf/project-files.transaction.json` is already the correct multi-file generation journal.
`publish_project_files_transactionally`:

1. validates project-relative regular paths;
2. stages and syncs every target and new ancestry;
3. persists an owner-only pending marker containing only path/length/hash/generation facts;
4. installs public targets with the declared commit path last;
5. accepts only journaled prior/new identities during recovery;
6. uses forward-only recovery under the `cdf.lock` mutation guard;
7. publishes a committed generation after target durability.

Read-only `ProjectContext` loading observes generation before/after and fails closed on pending.
Only explicitly mutating command paths use recovery. D1 should extend, not fork, this mechanism.

If a refresh changes both manifest and lock, the manifest is installed first and `cdf.lock` remains
the final public commit point. If an offline compile changes only the generated manifest, the
manifest itself is the transaction's final target. Stable-generation readers then see either the
old or new manifest, never an incomplete write.

### Current configuration smell confirmed

`DestinationPolicy` contains a first-class `postgres: Option<PostgresDestinationPolicy>` and
`PostgresMergeDedupPolicy`, while other adapters use a generic string map. Runtime and replay still
accept `merge_dedup` despite the active package-owned winner decision. This is both a leaked adapter
abstraction and active source/record drift. D0 should give its removal one bounded executable child;
it must not be preserved in the new compiler/profile model.

### Manifest query seam

The current in-memory SQLite implementation already enforces one read-only query, blocks mutating
keywords, avoids attaching arbitrary databases, and normalizes query results. D1 can add manifest
tables by parsing and verifying the canonical artifact, creating fixed in-memory tables, and
inserting typed rows. It should retain SQLite and the current CLI report shape.

The smallest useful initial public relation set is:

- `manifest_project` — one header/identity/lock/compiler/environment row;
- `manifest_inputs` — authored origin, kind, path/origin, hashes, parser versions;
- `manifest_resources` — identity, source, descriptor/capability/plan/schema/contract/destination
  hashes plus canonical JSON for complete nested facts;
- `manifest_fields` — resource/field ordinal, Arrow type/nullability/provenance/semantic binding;
- `manifest_semantics` — reachable definition, parameters, definition/profile hashes, usage;
- `manifest_lineage` — typed from/to nodes, relation kind, expression/opaque evidence;
- `manifest_diagnostics` — stable code/severity/location/blocking/remediation.

Hooks and generated expansions can enter the manifest model as empty typed collections now only if
the model already has a genuine current consumer; otherwise D1 should version-add them when those
lanes execute. No placeholder tables are needed.

## Recommended ratification

### Artifact and commands

- path: `.cdf/manifest.json`, generated and normally uncommitted;
- `cdf compile`: locked/offline compilation, no source/destination network contact, publishes only
  a manifest matching the current lock;
- `cdf compile --refresh`: explicit read-only external discovery/health as required, publishes the
  new manifest and changed `cdf.lock` in one transaction with `cdf.lock` last;
- neither command mutates destinations, state, packages, receipts, or checkpoints;
- `cdf sql` never compiles, refreshes, or recovers publication; it verifies and mounts the current
  selected-environment manifest or fails with exact compile/retry remediation;
- manifest retention is latest successful generation; content hashes and source control retain
  longer history when desired. Packages remain execution-history authority.

The scaffold should ignore `.cdf/` because it already contains local databases, package payloads,
secrets, transaction state, and the generated manifest. `cdf.lock` remains committed.

### SQL-shaped resource surface

Use one CDF-owned typed `CREATE RESOURCE ... AS SELECT ...` envelope:

- explicit resource id in the statement is authority; filename never derives it;
- files live under `resources/**/*.cdf.sql`; duplicate declared ids fail;
- named source profiles and connection/policy/secret-reference options live in `cdf.toml` and are
  validated through driver schemas;
- typed envelope clauses carry source relation, target, disposition, key, cursor, contract/trust,
  and execution facts; no generic `WITH` map and no companion metadata file;
- DataFusion parses/analyzes only the `SELECT` body; CDF parses the envelope and lowers the admitted
  subset into native CDF IR;
- destination connection remains environment authority, so SQL may name a logical target but
  cannot embed a destination URI or credential;
- declarative TOML/YAML remains a peer front-end until separately removed.

This keeps files SQL-shaped and explicit while preventing connection configuration from becoming
stringly SQL.

## Conclusions

D1 is an artifact-model and compiler-assembly task, not a configuration rewrite. Most canonical
facts already exist in typed Rust structures, and the publication/recovery protocol is mature.
The correct first cut is one generated, hashed manifest plus verified SQLite projection. D2/D3 can
then add native IR and SQL inputs without changing manifest identity fundamentals.

The Postgres destination-policy special case is real drift and should be removed as a small D0
child before the manifest freezes current policy facts.

## Limits

The inventory does not ratify the recommendations. It does not specify the full manifest JSON
schema, SQL token grammar, scalar IR allowlist, or project semantic-definition TOML schema; those
belong in active focused specs after the user confirms the user-visible choices. No external
source/destination was contacted.
