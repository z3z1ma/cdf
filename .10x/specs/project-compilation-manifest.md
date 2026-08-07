Status: active
Created: 2026-08-03
Updated: 2026-08-04

# Project compilation manifest

## Status and purpose

This specification defines the canonical derived output of compiling a CDF project. It is the
prerequisite for a SQL authoring front-end and plan-declared hooks. It binds exact state-backed
schema authority and compiler inputs without replacing either authority.

The core rule is:

> CDF MUST publish a stable, versioned, content-hashed, secret-redacted compilation artifact before
> it exposes that project graph through `cdf sql` or any future catalog service.

## Problem

Current compilation facts are distributed across:

- `cdf.toml`, path-derived SQL resources with explicit configured-source bindings, and project
  semantic definitions;
- active per-resource schema generations from environment state plus compiler/driver capability
  facts;
- in-memory `CompiledSourcePlan` and compiler bindings;
- compiled operator/contract/destination plans;
- package plan evidence produced only at run time;
- CLI inspect/deep-validation reports.

No single artifact answers, without recompiling or contacting sources:

- which authored files produced each resource;
- which exact source plan/options/schema/capabilities were compiled;
- which transforms/contracts/semantics/destination mappings will run;
- what lineage connects authored inputs to outputs;
- which hooks/templates/macros affected the plan;
- which hashes make two project compilations equivalent.

That omission is tolerable for the current declarative spike but not for a SQL project compiler.

## Authority boundaries

The state backend owns immutable active logical schema versions and promotion history. Authored
project files own intent. Driver catalogs own current declared capability sheets. The compilation
manifest is derived evidence binding the exact relevant state head, authored bytes, dependency
tuple, semantic definitions, source plan, contract program, and destination sheet.

The manifest is generated compiler output:

- complete resolved project graph for one environment/compiler tuple;
- exact authored-input digests and origins;
- exact compiled plans, schemas, contracts, semantics, hooks, and lineage;
- deterministic diagnostics and exclusions;
- one top-level content hash.

It may change whenever authored project semantics, active schema authority, dependencies, or driver
facts change. It never advances an established schema head.

## Artifact requirements

The manifest MUST be:

- versioned independently from state schema records, authored resource grammar, source plans, and packages;
- canonically serialized with deterministic map/list ordering;
- content-hashed over semantic content, excluding its own hash field and non-semantic timestamps;
- secret-redacted using source/destination option-schema authority;
- self-validating and fail closed on unknown versions or hash mismatch;
- atomically published through the project multi-file publication/crash-recovery authority;
- readable without loading database drivers or contacting external systems;
- bounded or stream-decodable for large projects;
- stable enough for diffing and tooling, while version changes explicitly mark schema evolution.

The artifact path is `.cdf/manifest.json`. It is generated local state and MUST NOT be committed by
the standard scaffold; the scaffold MUST ignore `.cdf/`.

## Compilation modes

Compilation must distinguish external observation from pure lowering.

### Ordinary compile

`cdf compile [selectors...]` prepares selected resources independently:

- read project files, active state authority, built-in driver/semantic catalogs, and referenced
  local artifacts;
- perform bounded source schema discovery only when selected first-use authority is absent;
- establish that exact missing baseline in state and publish derived artifacts;
- perform no destination, package, receipt, checkpoint, or run-ledger mutation.

### Authority-required compile

`cdf compile --locked` requires an existing unchanged state-backed schema head for every selected
resource. It performs no source schema discovery and cannot establish or advance authority.

## Manifest sections

### Header and identity

- manifest schema version;
- canonical manifest hash;
- project id/name and environment;
- compiler/CDF version and dependency tuple hash;
- normalizer and compiler policy versions;
- exact per-resource state authority key, domain, generation, and schema hash;
- compilation mode and deterministic feature/capability set;
- optional generated-at timestamp excluded from semantic identity.

### Authored inputs

For every input:

- normalized project-relative path or typed non-file origin;
- input kind (`project`, `resource_sql`, `semantic_definition`, `hook`, generated
  expansion, etc.);
- byte/content hash;
- parser/schema version;
- path-derived resource namespace/name/id/default target, explicit configured-source binding, and
  effective named-source configuration;
- bare-query versus expanded-envelope form, normalized authored AST hash, and effective normalized
  resource-definition hash;
- generated/explicit status and generator identity;
- no absolute host paths in canonical identity unless the active project policy requires them.

Manifest load MUST re-read every project-relative authored input under the project-root path
fence and verify its recorded content hash. Missing, replaced, escaped, or changed input authority
is stale manifest data and MUST fail before any manifest row is returned; query commands never
silently serve the last compiled view of changed authored files.

### Resources

For each canonical resource id:

- authoritative `cdf/<namespace>/<resource>.cdf.sql` origin, derived namespace/resource/id,
  default logical target, and expansion origin when a future explicit generator is used;
- exact authored SQL bytes/hash, bare/envelope form, normalized authored AST hash, and effective
  normalized definition/execution hash;
- effective target, disposition and merge keys, cursor, trust, semantic bindings, and execution
  policy, each with origin (`authored`, `project_default`, `built_in_default`, or
  `resource_path_default`), canonical typed identity, and authored span where present;
- explicit configured source name, selected immutable source type, exact driver descriptor/option-
  schema hashes, stable source-node id, driver-owned upstream relation identity, canonical typed
  structured relation arguments, and canonical secret-redacted base/overlay/effective source
  configurations;
- complete `ResourceDescriptor` and resource capabilities;
- execution extent and compiled stream policy;
- driver descriptor and option-schema hash;
- redacted source options plus hash;
- physical source plan plus typed physical/compiled/semantic hashes;
- source execution and stream capability hashes;
- discovered/declared schema observations and selected effective schema;
- output schema/provenance and schema hash;
- contract policy/program/snapshot references and hashes;
- semantic references/definition hashes used by each field;
- compiled transforms/operator graph and native expression/function versions;
- destination target, sheet, mapping, disposition, and compiled destination plan identity where
  planning is available without mutation;
- hook declarations and code/schema/capability identities;
- data/control lineage and source-position/watermark behavior;
- exclusions/unsupported features and diagnostics.

Configured source, canonical resource id, and logical target are separate manifest fields and
lineage nodes. No serializer, query projection, or display surface may infer one from another or
call the resource namespace a source. Authored identity and effective execution identity remain
separate: bare and explicitly enveloped resources may share execution identity only when all
resolved metadata, typed dependencies, and canonical policies are equal, while authored hashes
remain distinct.

Large sub-artifacts MAY be external content-addressed references, but the manifest MUST carry exact
type, byte count, hash, and required/optional semantics. A resident cache is never authority.
Destination lock binding MUST use the built-in composition root's canonical destination id for the
selected URI scheme; URI aliases such as `postgresql` and `clickhouses` are transport spellings,
not lockfile identities.

### Semantic registry snapshot

- every reachable semantic definition id/version/hash;
- source (built-in, adapter, project);
- normalized parameters actually used;
- Arrow compatibility/validation/redaction/mapping profile hashes;
- canonical semantic reference after direct producer migration; no alias resolution layer.

### Lineage

Lineage MUST be compiler-derived, not reconstructed from display SQL:

- resource-to-explicit-configured-source relation;
- resource-path-to-resource-namespace/id/default-target derivation;
- configured source-to-source-type/driver relation and source-to-upstream-relation selection;
- output field to input field(s) and transform expression id;
- contract rule to affected fields;
- semantic definition to fields and destination mapping;
- hook attach point/input/output schema relation;
- destination target/disposition relation;
- generated-resource origin/template expansion relation.

Unsupported/opaque lineage is explicit. CDF MUST NOT fabricate field lineage through arbitrary
foreign hooks.

### Diagnostics

Diagnostics SHOULD be stable records with:

- severity and stable code;
- resource/input location;
- message and remediation;
- authority that produced it;
- whether it blocks execution;
- no secrets or unredacted source values.

Non-semantic timing/performance telemetry belongs in command output, not manifest identity.

## Canonical hash model

The manifest SHOULD have layered typed hashes rather than one interchangeable string:

- authored-input-set hash;
- dependency/lock binding hash;
- resource compilation hashes;
- semantic snapshot hash;
- lineage hash;
- complete manifest hash.

Existing typed hashes—`CompiledSourcePlanHash`, `PhysicalSourcePlanHash`,
`SourceSemanticsHash`, schema/contract/operator/destination hashes—must be reused with their exact
meaning. The manifest MUST NOT recompute and relabel one category as another.

## Publication and crash recovery

Publication MUST reuse `.10x/knowledge/project-file-publication-recovery.md` exactly:

- Compilation writes every new public target to a private, owner-only managed temporary and syncs
  its new directory ancestry before publishing the transaction marker.
- Validation recomputes all hashes and cross-references before publication.
- The private `.cdf/project-files.transaction.json` marker contains version, generation, relative
  paths, lengths, and prior/new hashes only—never project content, manifest content, secrets, or
  signed URLs.
- Once a durable `pending` marker exists, forward completion is the only recovery decision. An
  in-memory rollback is forbidden.
- Every public target is accepted only at its journaled prior or new hash. A third value is
  unrelated authority: preserve it and fail `Contract`.
- When one command publishes multiple generated files, install immutable children first and the
  declared index/manifest commit target last, then mark the same generation committed. Stable load
  must observe one committed generation before using the index.
- Read-only/offline compile, plan, preview, inspect, and `cdf sql` MUST NOT recover a pending
  publication. They fail closed without mutation. Only an explicit mutating retry/refresh path may
  recover under the same project mutation guard.
- Project load samples the committed generation before and after parsing/compilation and retries on
  change so no caller receives a mixed project/manifest view.
- A failed compile before `pending` leaves the last valid manifest intact and removes only its own
  managed temporary. After `pending`, explicit recovery converges idempotently.

If immutable content-addressed child artifacts live under `.cdf/`, they additionally follow
`.10x/knowledge/content-addressed-sidecar-publication.md`: canonical bytes are hashed before naming,
installed with no-clobber/create-or-verify, and an existing target is accepted only after byte
identity verification.

## Query exposure

Current `cdf sql` mounts artifacts in an in-memory SQLite catalog. The first implementation MUST
add these read-only tables over the manifest using that existing engine:

- `manifest_project`;
- `manifest_inputs`;
- `manifest_resources`;
- `manifest_fields`;
- `manifest_semantics`;
- `manifest_lineage`;
- `manifest_diagnostics`.

`manifest_project` contains exactly one row. Nested source plans, contracts, destination facts, and
other structures remain available as canonical JSON columns on the owning resource/field rows
where normalization would lose meaning. Hook or generation tables are added only when those
features exist; empty speculative tables are forbidden. These names and column contracts are
versioned manifest-query API.

`cdf sql` MUST locate the project and selected environment without compiling resource files, then
verify and mount the matching manifest. It MUST NOT contact a registry/source/destination, compile,
refresh, publish, or recover an interrupted publication.

Future DataFusion/ADBC/catalog serving MAY expose the same artifact; it MUST NOT become a second
compiler or reinterpret identity.

## Security

- Secret values and credentials MUST never enter authored-input excerpts, options, diagnostics,
  SQL text normalization, hook configuration, or lineage.
- Named secret references may be recorded only under existing redaction rules.
- SQL files MUST contain exactly one configured-source name in the reserved
  `upstream(source => '<name>', ...)` argument. Source types, driver ids, connection configuration,
  credentials, secret references/values, environment endpoints, and source-level options in SQL
  fail under the project/source authority split.
- Absolute host paths, environment values, and usernames are excluded or normalized unless they
  are intentionally semantic project inputs.
- Hook code is recorded by content hash and project-relative reference; embedded code bytes are a
  separate policy choice.
- Query exposure remains read-only and retains the current mutating-keyword and filesystem-shape
  protections.

## Failure behavior

- missing/stale locked authority: Contract/Data drift before lowering, with refresh remediation;
- unknown manifest/child artifact version: Data and fail closed;
- hash mismatch or dangling required reference: Data corruption;
- compiler constructs inconsistent typed hashes/cross-references: Internal;
- unrelated public target or caller-expectation drift: Contract and never overwrite a concurrent
  user edit;
- corrupt/missing CDF-private marker or managed temporary: Internal;
- local permission, capacity, descriptor, or device failure: Environment;
- secret discovered in a field governed as redacted: Internal stop-line before publication.

## Acceptance scenarios

1. Given unchanged project files, lockfile, compiler tuple, and catalogs, two offline compiles emit
   byte-identical semantic manifest content and the same hash.
2. Given only a non-semantic timestamp changes, the manifest hash remains stable.
3. Given a source plan, contract, semantic definition, hook, or SQL expression changes, the exact
   affected child/resource/top-level hashes change and unrelated resources remain stable.
4. Given a secret value changes behind the same secret reference, the manifest contains neither
   value and changes only if existing compiled semantics intentionally include a redacted identity.
5. Given missing schema authority, offline compile performs no network I/O and tells the user which
   explicit refresh is required.
6. Given a crash during multi-file publication, read-only project load and offline compile fail
   without recovery, while an explicit `cdf compile --refresh` retry recovers one coherent old or
   new lock/manifest pair.
7. Given the manifest is tampered with, validation and `cdf sql` reject it before returning rows.
8. Given `cdf sql` queries resources, semantics, or lineage, results come from the published
   artifact without recompiling or contacting a source.
9. Given two resources lower equivalent native operator fragments, the fragment hashes may match
   only where typed identity excludes resource identity; their path-derived resource ids and
   authored origins remain distinct.
10. Given any project-relative authored input changes after compilation, `cdf sql` rejects the
    stale manifest without writing or recompiling.
11. Given a resource namespace differs from its configured source, the manifest records both
    independently and derives neither from the other.
12. Given any omitted D3 metadata, the manifest records the resolved effective value and its exact
    origin before execution can consume the resource.
13. Given equivalent structured `upstream(...)` arguments in different orders, canonical typed
    relation identity is equal while authored SQL hashes remain distinct.
14. Given equivalent bare and expanded resource files, execution identity is equal only when all
    effective values and policies are equal; authored identity remains different.

## Explicit exclusions

- making JSON the runtime execution authority;
- replacing typed Rust compiler artifacts with unvalidated generic values;
- mutating external sources/destinations during ordinary compile;
- putting every package/checkpoint/receipt row inside the project manifest;
- unstable DataFusion debug/physical plan serialization as identity;
- runtime template expansion or hook code lookup;
- mandatory publication to a remote service.

## Ratified first implementation

- `.cdf/manifest.json` retains the latest successful selected-environment compilation. Packages,
  receipts, source control, or explicit copies retain historical evidence; D1 adds no manifest
  history database.
- The first artifact is one canonical bounded JSON document. Content-addressed children are added
  only after a measured project exceeds the declared manifest bound or another feature requires
  independently addressable payloads.
- Manifest-only publication uses the manifest/index as the final transaction target. Compilation
  installs immutable resource artifacts before that index.
- The manifest carries full reachable semantic definitions, normalized parameters, and usage
  snapshots; the compiled artifact hash binds those exact facts.
- The seven tables above are the complete D1 SQLite surface.
- The final Foundation D project compiler uses
  `.10x/specs/project-source-resource-layout.md`: the sole resource root is
  `cdf/<namespace>/<resource>.cdf.sql`; each query explicitly binds its configured source;
  the literal `cdf/` root is excluded from resource identity, and there is no root wildcard
  resource map, retired declarative/project reader, path-inferred source,
  explicit SQL resource id, or compatibility mode in current authority.

## References

- `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md`
- `.10x/decisions/compiled-output-schema-and-runtime-provenance.md`
- `.10x/decisions/source-driver-registry-and-resource-plan-boundary.md`
- `.10x/specs/datafusion-currency-bridges.md`
- `.10x/specs/semantic-type-registry.md`
- `.10x/specs/project-source-resource-layout.md`
- `.10x/decisions/filesystem-source-resource-and-configuration-authority.md`
- `.10x/decisions/project-path-tokens-and-upstream-relation-binding.md`
- `.10x/knowledge/project-file-publication-recovery.md`
- `.10x/knowledge/content-addressed-sidecar-publication.md`
- `VISION.md` D-19
