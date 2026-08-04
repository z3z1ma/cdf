Status: active
Created: 2026-08-03
Updated: 2026-08-03

# Semantic reference, registry, and unknown policy

## Context

CDF currently stores an arbitrary string in Arrow `cdf:semantic` metadata. Six behavior-bearing
families have emerged: PII redaction, variant ownership, package-row ordinal ownership, and exact
PostgreSQL JSON/JSONB/NUMERIC text reconstruction. Consumers independently use prefixes or magic
strings, while declarative authors can supply any value. The detailed inventory is
`.10x/research/2026-08-03-semantic-authority-inventory.md`.

Semantic types must remain annotations over Arrow rather than create another scalar type system.
CDF is pre-production and customer zero, so carrying aliases or accepting ambiguous legacy values
would create debt without preserving a supported contract.

## Decision

Canonical references use `namespace.name@version` with optional lexically sorted, definition-
declared canonical JSON scalar parameters. Namespace/name/parameter keys are lowercase ASCII snake
identifiers and version is a positive `u32`. There are no aliases, unversioned references, or
runtime “latest” resolution.

Every `cdf:semantic` value is behavior-bearing and must resolve against an immutable definition.
Authored unknowns fail `Contract`; unapproved source-observed unknowns fail `Data`; a runtime
reference absent from its compiled snapshot is `Internal`. Behavior-free descriptive tags do not
belong in this metadata key.

The initial current-format replacement is:

- `cdf.variant@1`;
- `cdf.package_row_ordinal@1`;
- parameterized `cdf.pii@1(class="...")`;
- `postgres.json_text@1`;
- `postgres.jsonb_text@1`;
- `postgres.numeric_text@1`.

Definitions are data-only: Arrow compatibility, closed validation/redaction facts, parameter
schema, equivalence/cast classification, and destination mapping profile selectors. Adapter crates
continue to own native DDL/encoding/verification. Project-defined definitions are included after
built-in migration using the same closed schema; no executable predicate or dynamic registry is
admitted.

`cdf.lock` pins reachable canonical reference → definition hash expectations. The project
compilation manifest carries full reachable definitions, normalized parameters, and field usage.

## Alternatives considered

### Preserve legacy aliases at authoring time

Rejected. CDF has no supported installed artifact base, aliases make misspellings harder to detect,
and dual representations undermine exact schema and manifest identity.

### Permit unknown descriptive semantics

Rejected. The same metadata key currently controls privacy and lossless reconstruction. A consumer
cannot safely prove an unknown value is merely descriptive.

### Encode semantics as new Arrow extension types

Rejected. Arrow is already the canonical closed physical/logical type system. Extension types would
duplicate kernels and destination mappings while making ordinary Arrow interchange harder.

### Put adapter callbacks in definitions

Rejected. It would make registry resolution executable, load-order-dependent, and unsuitable for
offline deterministic compilation. Mapping profile ids preserve the adapter seam without code in
the definition.

## Consequences

Existing artifact/schema hashes intentionally change once. Every producer, consumer, fixture, and
golden must migrate directly; no legacy reader or fallback remains. Misspelled PII or exact-value
semantics fail early instead of silently weakening policy. New destinations can refine Arrow
mapping through one resolved semantic authority without importing source crates.
