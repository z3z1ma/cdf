Status: draft
Created: 2026-08-03
Updated: 2026-08-03

# Project compilation manifest

## Status and purpose

This draft defines the canonical output of compiling a CDF project. It is the prerequisite for a
SQL authoring front-end and plan-declared hooks. It complements `cdf.lock`; it does not replace or
silently broaden lockfile semantics.

The core rule is:

> CDF MUST publish a stable, versioned, content-hashed, secret-redacted compilation artifact before
> it exposes that project graph through `cdf sql` or any future catalog service.

## Problem

Current compilation facts are distributed across:

- `cdf.toml` and declarative resource files;
- `cdf.lock` dependency/resource/schema/contract/destination pins;
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

## Lockfile versus manifest

### `cdf.lock`

The lockfile remains committed expectation/pin authority:

- CDF and dependency tuple;
- expected resource/capability/schema/contract facts;
- execution extent/stream policy;
- destination sheet/protocol capability pins;
- future semantic definition pins.

It changes when a user intentionally accepts dependency or semantic drift.

### Compilation manifest

The manifest is generated compiler output:

- complete resolved project graph for one environment/compiler tuple;
- exact authored-input digests and origins;
- exact compiled plans, schemas, contracts, semantics, hooks, and lineage;
- deterministic diagnostics and exclusions;
- one top-level content hash.

It may change whenever authored project semantics change even when dependency pins remain constant.
The compiler validates it against `cdf.lock`; it does not mutate lock expectations implicitly.

## Artifact requirements

The manifest MUST be:

- versioned independently from `cdf.lock`, declarative documents, source plans, and packages;
- canonically serialized with deterministic map/list ordering;
- content-hashed over semantic content, excluding its own hash field and non-semantic timestamps;
- secret-redacted using source/destination option-schema authority;
- self-validating and fail closed on unknown versions or hash mismatch;
- atomically published through the project multi-file publication/crash-recovery authority;
- readable without loading database drivers or contacting external systems;
- bounded or stream-decodable for large projects;
- stable enough for diffing and tooling, while version changes explicitly mark schema evolution.

The exact path and whether generated manifests are committed are unratified. A logical artifact name
such as `cdf.manifest.json` is recommended; path policy belongs to the CLI/project spec.

## Compilation modes

Compilation must distinguish external observation from pure lowering.

### Locked/offline compile

Recommended default:

- read project files, `cdf.lock`, built-in driver/semantic catalogs, and referenced local artifacts;
- perform no external network I/O and no destination mutation;
- lower only when all required source schema/capability/semantic authority is already locked;
- fail with exact missing/stale authority and a separately explicit refresh command when not.

### Refresh compile

A separately explicit mode may run source discovery/health to refresh schema/catalog observations.
It is read-only with respect to external sources but can publish updated project/lock/manifest files
only under the existing atomic project publication contract and user-authorized command semantics.

The exact CLI spelling is not ratified. `cdf compile` is the recommended pure command; refresh
should not be an implicit side effect of every compile.

## Manifest sections

### Header and identity

- manifest schema version;
- canonical manifest hash;
- project id/name and environment;
- compiler/CDF version and dependency tuple hash;
- normalizer and compiler policy versions;
- `cdf.lock` content/semantic hash;
- compilation mode and deterministic feature/capability set;
- optional generated-at timestamp excluded from semantic identity.

### Authored inputs

For every input:

- normalized project-relative path or typed non-file origin;
- input kind (`project`, `resource_sql`, `declarative`, `semantic_definition`, `hook`, generated
  expansion, etc.);
- byte/content hash;
- parser/schema version;
- originating source/resource mapping;
- generated/explicit status and generator identity;
- no absolute host paths in canonical identity unless the active project policy requires them.

### Resources

For each canonical resource id:

- authored origin(s) and expansion origin;
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

Large sub-artifacts MAY be external content-addressed references, but the manifest MUST carry exact
type, byte count, hash, and required/optional semantics. A resident cache is never authority.

### Semantic registry snapshot

- every reachable semantic definition id/version/hash;
- source (built-in, adapter, project);
- normalized parameters actually used;
- Arrow compatibility/validation/redaction/mapping profile hashes;
- legacy alias resolution where applicable.

### Lineage

Lineage MUST be compiler-derived, not reconstructed from display SQL:

- resource-to-source relation;
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
- When one command changes both a generated manifest and `cdf.lock`, install and sync the manifest
  target first and install **`cdf.lock` last as the public commit point**, then mark the same
  generation committed. The manifest cannot authorize execution until stable project load observes
  the matching committed generation and lock binding.
- Read-only/offline compile, plan, preview, inspect, and `cdf sql` MUST NOT recover a pending
  publication. They fail closed without mutation. Only an explicit mutating retry/refresh path may
  recover under the same project mutation guard.
- Project load samples the committed generation before and after parsing/compilation and retries on
  change so no caller receives a mixed lock/manifest view.
- A failed compile before `pending` leaves the last valid manifest intact and removes only its own
  managed temporary. After `pending`, explicit recovery converges idempotently.

If immutable content-addressed child artifacts live under `.cdf/`, they additionally follow
`.10x/knowledge/content-addressed-sidecar-publication.md`: canonical bytes are hashed before naming,
installed with no-clobber/create-or-verify, and an existing target is accepted only after byte
identity verification.

## Query exposure

Current `cdf sql` mounts artifacts in an in-memory SQLite catalog. The first implementation SHOULD
add read-only tables/views over the manifest using that existing engine, for example:

- `manifest_projects`;
- `manifest_inputs`;
- `manifest_resources`;
- `manifest_fields`;
- `manifest_source_plans`;
- `manifest_contracts` and `manifest_rules`;
- `manifest_semantics`;
- `manifest_lineage`;
- `manifest_hooks`;
- `manifest_diagnostics`.

Exact names are not ratified. Tables must preserve stable column contracts and expose canonical
JSON for nested facts where normalization would lose meaning.

Future DataFusion/ADBC/catalog serving MAY expose the same artifact; it MUST NOT become a second
compiler or reinterpret identity.

## Security

- Secret values and credentials MUST never enter authored-input excerpts, options, diagnostics,
  SQL text normalization, hook configuration, or lineage.
- Named secret references may be recorded only under existing redaction rules.
- SQL files that contain credential-shaped literals SHOULD fail compilation when the profile split
  forbids them.
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
6. Given a crash during multi-file publication, project load recovers one coherent old or new
   lock/manifest pair.
7. Given the manifest is tampered with, validation and `cdf sql` reject it before returning rows.
8. Given `cdf sql` queries resources, semantics, or lineage, results come from the published
   artifact without recompiling or contacting a source.
9. Given declarative and SQL front-ends lower to identical native plans, their resource compilation
   hashes match while authored-origin metadata remains distinguishable outside execution identity.

## Explicit exclusions

- making JSON the runtime execution authority;
- replacing typed Rust compiler artifacts with unvalidated generic values;
- mutating external sources/destinations during ordinary compile;
- putting every package/checkpoint/receipt row inside the project manifest;
- unstable DataFusion debug/physical plan serialization as identity;
- runtime template expansion or hook code lookup;
- mandatory publication to a remote service.

## Open blockers

- artifact path, commit/ignore policy, and retention;
- exact offline/refresh command grammar;
- manifest schema normalization versus content-addressed child artifacts at large scale;
- project-file publication ordering integration;
- initial `cdf sql` table names/columns;
- how `cdf.lock` pins semantic definitions without duplicating the full reachable snapshot.

## References

- `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md`
- `.10x/decisions/compiled-output-schema-and-runtime-provenance.md`
- `.10x/decisions/source-driver-registry-and-resource-plan-boundary.md`
- `.10x/specs/datafusion-currency-bridges.md`
- `.10x/specs/semantic-type-registry.md`
- `.10x/knowledge/project-file-publication-recovery.md`
- `.10x/knowledge/content-addressed-sidecar-publication.md`
- `VISION.md` D-19
