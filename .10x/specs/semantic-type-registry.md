Status: draft
Created: 2026-08-03
Updated: 2026-08-03

# Semantic type registry

## Status and design invariant

This draft proposes first-class semantic-definition authority. It MUST remain subordinate to the
active Arrow type invariant:

> Arrow is CDF's closed canonical physical/logical data type system. A semantic type is a
> versioned annotation profile over an Arrow field, not a parallel value representation or custom
> kernel type lattice.

The registry is not executable until namespace/version syntax, project-defined type scope, and
migration of existing free-form tags are ratified.

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

## Existing behavior that must be preserved

Current semantics are already identity- and correctness-bearing:

- `pii:*` selects PII redaction policy;
- the framework variant column uses a fixed semantic tag;
- PostgreSQL JSON, JSONB, and NUMERIC exact-value text tags combine with `cdf:physical_type` to
  permit lossless reconstruction in the Postgres destination;
- schema declarations and Arrow metadata preserve arbitrary semantic strings;
- tests reject semantic reinterpretation even where destination SQL types match.

The registry migration MUST inventory every emitted/consumed tag, assign a canonical definition or
legacy alias, and explicitly account for schema/package/hash changes. Unknown legacy tags cannot be
silently reinterpreted.

## Semantic reference

Every semantic-bearing field SHOULD resolve to a canonical reference with:

- namespace;
- stable name;
- explicit definition version;
- optional validated parameters;
- definition content hash in the compiled/locked registry snapshot.

Recommended human form: `namespace.name@version` plus canonical parameters. The exact grammar is
unratified. The serialized Arrow metadata SHOULD carry a compact canonical reference and the
compiled plan/manifest MUST bind it to the full definition hash.

Unversioned aliases MAY be accepted only at authoring time and MUST lower immediately to a pinned
version. Runtime/package artifacts MUST NOT depend on “latest.”

## Definition model

A semantic definition MUST be data, not arbitrary executable code. It SHOULD include:

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

CDF SHOULD ship a small built-in registry for semantics already present in source:

- framework variant;
- PII classifications currently represented by `pii:*`;
- exact PostgreSQL JSON/JSONB/NUMERIC text profiles;
- any canonical CDC operation/key semantics approved by the CDC contract.

Built-ins are versioned with CDF and included in the dependency tuple/manifest snapshot.

### Adapter-contributed definitions

A first-party adapter MAY contribute definitions only through the built-in composition root and a
data-only descriptor validated for uniqueness. Generic runtime code MUST NOT branch on the adapter
id. Conflicting ids/versions/hashes fail catalog construction.

### Project-defined definitions

Project-defined semantic types are desirable but add compatibility, sharing, trust, and validation
surface. Recommended staging:

1. first implement built-ins, exact resolution, locking, and all existing consumers;
2. then admit project definitions using the same data-only schema and closed validation vocabulary;
3. later consider signed external registries after a concrete distribution requirement.

Whether project definitions are part of the first slice is an explicit ratification blocker.

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

Current destination mappings select only by Arrow type. The registry program SHOULD extend the
shared mapping model to distinguish:

- base Arrow mapping;
- optional semantic-specific refinement;
- physical-provenance prerequisites;
- deterministic specificity/ambiguity rules.

Resolution order SHOULD be most-specific valid semantic+Arrow+parameter mapping, then base Arrow
mapping only when the semantic permits fallback. Equal-specificity matches are a contract error.

An unknown semantic MUST NOT be ignored when it claims exact-value or control-critical meaning. For
pure descriptive legacy tags, fallback behavior requires an explicit migrated definition.

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

- Semantic metadata is untrusted at source/declarative boundaries and must resolve against the
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
4. Given an exact PostgreSQL JSONB text field, its legacy tag resolves to a canonical definition and
   Postgres destination reconstruction remains lossless and rejects incompatible physical
   provenance.
5. Given `pii:email`, migration preserves the current PII action while replacing prefix inference
   with registry classification.
6. Given an unknown semantic, no adapter silently treats it as ordinary Utf8 when exact-value or
   privacy meaning may be lost.
7. Given a project-defined semantic is not enabled in the first slice, the compiler emits an exact
   remediation rather than accepting a free-form tag.
8. Given SQL and declarative front-ends author the same field semantics, both lower to identical
   resolved schema/contract/manifest identities.

## Migration plan requirements

The executable program MUST first generate an inventory of all current semantic strings and
consumers. It then MUST:

1. define canonical built-ins and legacy aliases;
2. add registry resolution without changing behavior;
3. migrate redaction and destination mapping consumers to resolved definitions;
4. add lock/manifest identity;
5. reject unknown required semantics;
6. remove free-form prefix/magic-string decisions only after equivalence tests pass.

Golden fixtures must distinguish intended artifact-version/hash changes from accidental data or
redaction changes.

## Open blockers

- canonical id/version/parameter syntax;
- first-slice project-defined type support;
- exact legacy tag inventory and alias policy;
- whether descriptive unknown tags remain allowed and, if so, with what no-behavior contract;
- destination-sheet selector schema and compatibility version;
- lockfile versus manifest placement for the reachable registry snapshot.

## References

- `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md`
- `.10x/specs/types-contracts-normalization.md`
- `.10x/knowledge/type-policy-authority.md`
- `.10x/decisions/compiled-output-schema-and-runtime-provenance.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `VISION.md` D-15
