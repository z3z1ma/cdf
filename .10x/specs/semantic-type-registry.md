Status: active
Created: 2026-08-03
Updated: 2026-08-04

# Semantic type registry

## Status and design invariant

This specification defines first-class semantic-definition authority. It remains subordinate to the
active Arrow type invariant:

> Arrow is CDF's closed canonical physical/logical data type system. A semantic type is a
> versioned annotation profile over an Arrow field, not a parallel value representation or custom
> kernel type lattice.

The namespace/version syntax, fail-closed unknown policy, direct replacement map, and inclusion of
data-only project definitions were user-ratified on 2026-08-03.

## Purpose

Turn the existing `cdf:semantic` string from an adapter-local convention into a versioned,
content-addressed compile authority shared by:

- schema/declarative compilation;
- contract validation and normalization;
- redaction and display policy;
- destination type mapping and fidelity;
- quarantine/evidence rendering;
- SQL authoring annotations/casts;
- hook schema declarations;
- project lock/manifest/package identity.

One definition should explain what a semantic means everywhere instead of allowing contracts,
destinations, and adapters to infer different behavior from the same string.

## Historical behavior whose meaning must be preserved

Before direct migration, semantics were already identity- and correctness-bearing:

- the former `pii:*` prefix selected PII redaction policy;
- the framework variant column uses a fixed semantic tag;
- PostgreSQL JSON, JSONB, and NUMERIC exact-value text tags combine with `cdf:physical_type` to
  permit lossless reconstruction in the Postgres destination;
- schema declarations and Arrow metadata preserve arbitrary semantic strings;
- tests reject semantic reinterpretation even where destination SQL types match.

The registry replacement MUST inventory every emitted/consumed tag, assign each retained behavior
one canonical definition, update every producer/consumer/fixture directly, and explicitly account
for schema/package/hash changes. No alias or compatibility resolution layer is permitted.

## Semantic reference

Every semantic-bearing field MUST resolve to a canonical reference with:

- namespace;
- stable name;
- explicit definition version;
- optional validated parameters;
- definition content hash in the compiled/locked registry snapshot.

The canonical human and artifact form is:

```text
namespace.name@version
namespace.name@version(key=<canonical-json-scalar>,...)
```

`namespace`, `name`, and parameter keys MUST be lowercase ASCII snake identifiers. `version` MUST
be a positive `u32` definition version. Parameter keys MUST be unique, declared by the definition,
and serialized in lexical order; values MUST be canonical JSON strings, numbers, or Booleans.
Arrays, objects, null, duplicate keys, whitespace variants, unknown parameters, and noncanonical
escaping/numbers MUST fail compilation. The serialized Arrow metadata carries this pinned canonical
reference and the compiled plan/manifest binds it to the full definition hash.

Aliases and unversioned references are not accepted. An id/version definition is immutable; a
behavior change requires a new version. Runtime/package artifacts MUST NOT depend on “latest.”

## Definition model

A semantic definition MUST be data, not arbitrary executable code. It MUST include:

### Identity

- canonical id and version;
- definition schema version;
- description and owning namespace;
- canonical definition hash;
- optional supersedes/deprecation metadata.

### Arrow compatibility

- one or more allowed canonical Arrow type patterns;
- nullability constraints only when semantically essential;
- required companion metadata such as `cdf:physical_type`;
- parameter schema and canonical parameter normalization;
- nested propagation rules where meaningful.

One semantic reference cannot authorize changing the underlying Arrow type at runtime. A field
whose Arrow type is incompatible fails compilation/discovery.

### Validation

- deterministic native validation predicates compiled into CDF's validation program;
- stable rule ids and error codes;
- whether invalid values quarantine or fail under each contract/trust policy already defined by
  CDF;
- control-critical status where invalid/missing values cannot be quarantined safely;
- optional canonicalization only when it is lossless and identity-recorded.

Definitions MUST use a closed built-in predicate vocabulary. Python/WASM callbacks and arbitrary
regex/function code are excluded from registry definitions until a separate deterministic
extension contract exists.

### Privacy and presentation

- redaction class (for example PII category) independent of the human tag prefix;
- default display/quarantine evidence action constrained by the active contract policy;
- whether values may appear in explain/telemetry samples;
- stable masking/hashing strategy identifiers, never secrets.

The registry supplies classification; trust/contract policy remains the authority that selects the
actual action. A semantic cannot weaken a stricter policy.

### Equivalence and casts

- semantic equivalence rules for same-Arrow representations;
- explicit, directional semantic casts with lossless/lossy/unsupported classification;
- parameter compatibility rules, such as currency/unit identity;
- whether dropping the semantic is permitted and under which explicit contract allowance.

No implicit cast may change data meaning. Destination-native reconstruction from an exact-value
text representation is a destination mapping, not permission for native CDF operators to treat
Utf8 as a new scalar type.

### Destination mapping overrides

- optional destination id/version selectors;
- required destination native type/family or mapping profile id;
- fidelity classification;
- adapter-owned encoder/decoder profile id;
- prerequisites such as physical provenance, server capability, extension, precision, or scale;
- explicit unsupported cases.

The registry may select or refine a destination mapping but MUST NOT contain SQL, connection
settings, or adapter code. Concrete destination crates remain responsible for DDL/DML, binary
encoding, and verification.

## Built-in, project, and adapter definitions

### Built-ins

CDF MUST ship this initial built-in registry:

- `cdf.variant@1` for the nullable UTF-8 framework residual column;
- `cdf.package_row_ordinal@1` for the non-null internal `UInt64` package ordinal;
- `cdf.pii@1(class="...")` preserving the current any-Arrow-type PII redaction behavior;
- `postgres.json_text@1`, `postgres.jsonb_text@1`, and `postgres.numeric_text@1` for exact
  PostgreSQL values with their existing UTF-8 and physical-provenance prerequisites.

The former `json`, `package-row-ord-v1`, `pii:*`, and `postgres_*_value_text_v1` strings have been
replaced directly and MUST NOT be accepted. Descriptive `id` and other behavior-free tags are
removed rather than aliased. Approved future CDC control semantics may add separately versioned
definitions.

Built-ins are versioned with CDF and included in the dependency tuple/manifest snapshot.

### Adapter-contributed definitions

A first-party adapter MAY contribute definitions only through the built-in composition root and a
data-only descriptor validated for uniqueness. Generic runtime code MUST NOT branch on the adapter
id. Conflicting ids/versions/hashes fail catalog construction.

### Project-defined definitions

Project-defined semantic types are in scope and add compatibility, sharing, trust, and validation
surface. Required staging:

1. first implement built-ins, exact resolution, locking, and all existing consumers;
2. then admit project definitions using the same data-only schema and closed validation vocabulary;
3. later consider signed external registries after a concrete distribution requirement.

Project definitions MUST use the same data-only schema and closed predicate/mapping vocabulary as
built-ins. They are implemented after built-in migration and manifest publication are stable.

## Compilation and identity

Semantic resolution occurs before contract and destination compilation:

```text
authored/discovered Arrow field + semantic reference
→ resolve exact definition/version/parameters
→ validate Arrow compatibility
→ compile semantic validation/redaction/mapping facts
→ bind definition hash into output schema, contract, destination plan, and manifest
```

The compiler MUST produce a canonical semantic snapshot containing only definitions reachable from
the project. The snapshot and per-field resolved references are content-hashed.

The following identities MUST change when behavior-bearing semantic authority changes:

- compiled output schema/provenance as governed by existing schema identity rules;
- contract validation/redaction program hash;
- destination mapping/plan hash where mapping changes;
- project compilation manifest hash;
- package/plan evidence that already binds the relevant schema/contract/plan.

The lockfile pins expected semantic definitions; the compilation manifest records exact resolved
usage. No runtime network lookup or mutable global registry is permitted.

## Destination-sheet evolution

Current destination mappings select only by Arrow type. The registry program MUST extend the
shared mapping model to distinguish:

- base Arrow mapping;
- optional semantic-specific refinement;
- physical-provenance prerequisites;
- deterministic specificity/ambiguity rules.

Each semantic destination selector MUST name an exact destination id, allowed Arrow pattern,
adapter-owned mapping profile id, fidelity, required metadata predicates, and whether base Arrow
fallback is legal. Resolution order is the most-specific valid semantic+Arrow+parameter mapping,
then base Arrow mapping only when the definition explicitly permits fallback. Equal-specificity or
conflicting matches are a Contract error.

An unknown semantic MUST NOT be ignored when it claims exact-value or control-critical meaning.
Descriptive tags retained by the new contract require a canonical definition; unneeded old tags are
deleted rather than aliased.

## SQL authoring surface

The SQL project front-end MAY expose semantic annotations or cast-like syntax, but it MUST lower to
the same resolved semantic reference and ordinary Arrow type. It MUST NOT let DataFusion invent or
execute an unrecorded user-defined type.

Illustrative, not ratified:

```sql
CAST(amount AS DECIMAL(38, 9)) /* semantic: finance.money@1(currency='USD') */
```

Exact grammar belongs to the SQL authoring spec. `CREATE TYPE` is excluded from the initial parser
unless it creates a data-only registry definition and cannot introduce runtime code or a new
physical representation.

## Schema discovery and drift

- A source-discovered semantic must identify the adapter definition/version that produced it.
- Changing only a source-native physical type may still be semantic drift if definition
  prerequisites change.
- Changing a semantic reference/version is contract/schema drift even when Arrow type is unchanged.
- Discovery MUST fail early when a source emits an unknown required semantic definition.
- Variant capture MUST preserve the original semantic/physical provenance without allowing it to
  forge framework metadata on accepted fields.

## Security and redaction

- Semantic metadata is untrusted at source/authored-project boundaries and must resolve against the
  locked registry before it affects redaction.
- A source cannot evade PII policy by supplying an unknown or misspelled semantic.
- Registry definitions cannot contain credentials, executable code, filesystem paths, or network
  references.
- Explain, JSON reports, manifests, packages, and quarantine artifacts use the same compiled
  redaction classification.
- Policy may tighten a definition's default; it may not weaken mandatory classification without an
  explicit higher-authority decision.

## Failure behavior

- unknown definition/version: Contract for authored configuration; Data for an unapproved
  source-observed semantic;
- Arrow incompatibility or invalid parameters: Contract before execution;
- destination mapping ambiguity/unsupported semantic: Contract before destination mutation;
- registry hash/lock mismatch: Data drift with exact expected/observed ids and hashes;
- runtime semantic reference absent from the compiled snapshot: Internal, because compilation was
  bypassed or artifacts are inconsistent;
- invalid value: existing contract verdict/quarantine semantics using the definition's stable rule
  ids and active policy.

## Acceptance scenarios

1. Given a built-in semantic and compatible Arrow field, compilation pins one exact definition and
   every consumer observes the same id/version/hash.
2. Given the same Arrow type with two semantic definitions, destination mapping and redaction may
   differ only according to their resolved definitions and both results are manifest-recorded.
3. Given a semantic version changes validation or mapping, lock/manifest/contract/plan drift is
   visible even when the Arrow schema text is unchanged.
4. Given an exact PostgreSQL JSONB text field, its producer emits the canonical definition directly
   and Postgres destination reconstruction remains lossless and rejects incompatible physical
   provenance.
5. Given a historical `pii:email` field, direct migration preserves the PII action through
   `cdf.pii@1(class="email")` while deleting prefix inference and rejecting the historical spelling.
6. Given an unknown semantic, no adapter silently treats it as ordinary Utf8 when exact-value or
   privacy meaning may be lost.
7. Given a project-defined parameterized semantic such as `finance.currency@1(code="USD")`, the
   project compiler validates the closed data-only definition and parameters, binds it to the exact
   output field, and snapshots the full definition/reference/usage in lock and manifest authority.
8. Given SQL authors a semantic reference, it lowers through the same registry resolution and field
   binding used by every compiler producer; there is no authoring-format-specific semantic model.

## Migration plan requirements

The executable program MUST first generate an inventory of all current semantic strings and
consumers. It then MUST:

1. define canonical built-ins and update producers directly, with no aliases;
2. add registry resolution while preserving the intended current behavior;
3. migrate redaction and destination mapping consumers to resolved definitions;
4. add lock/manifest identity;
5. reject unknown required semantics;
6. remove free-form prefix/magic-string decisions only after equivalence tests pass.

Golden fixtures must distinguish intended artifact-version/hash changes from accidental data or
redaction changes.

## Ratified staging

- C1 implements canonical parsing, built-ins, direct producer/consumer migration, validation,
  redaction classification, and destination mapping profiles.
- D1 adds `CdfLock.semantics`, a map from each reachable canonical reference to its definition
  hash, while the manifest records the complete reachable definitions, normalized parameters, and
  per-field usage. The lock therefore pins expectation without duplicating the snapshot.
- C2 adds project definition files through the Foundation D project compiler after C1 and D1 are
  stable. Their exact authored file grammar/path remains a focused C2 contract, but they use the
  existing data-only `SemanticDefinition` model and cannot add executable type behavior.
- D3's ratified `SEMANTICS (output_column => 'canonical.reference', ...)` syntax remains owned by
  `.10x/specs/sql-project-authoring.md`; it resolves each exact final output field through this
  registry, records definition/version/hash/parameters and consumer effects, changes no physical
  representation, and introduces no alternate semantic model.

## References

- `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md`
- `.10x/specs/types-contracts-normalization.md`
- `.10x/knowledge/type-policy-authority.md`
- `.10x/decisions/compiled-output-schema-and-runtime-provenance.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/project-source-resource-layout.md`
- `VISION.md` D-15
