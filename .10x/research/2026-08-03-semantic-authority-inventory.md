Status: done
Created: 2026-08-03
Updated: 2026-08-03

# Semantic metadata authority inventory

## Question

Which semantic values does current CDF produce, accept, interpret, preserve, or use as identity,
and what exact replacement boundary is required before the semantic registry can become
executable?

## Sources and methods

Inspected every Rust occurrence of `cdf:semantic`, `SEMANTIC_METADATA_KEY`, `semantic(...)`, and
`with_semantic(...)`, then followed behavior-bearing values into their producers and consumers.
The focused authority paths were:

- `crates/cdf-kernel/src/metadata.rs`;
- `crates/cdf-declarative/src/declarations.rs` and `compiled.rs`;
- `crates/cdf-contract/src/compiler.rs`, `policy.rs`, and `program.rs`;
- `crates/cdf-postgres/src/lib.rs`;
- `crates/cdf-source-postgres/src/catalog.rs` and `source.rs`;
- `crates/cdf-dest-postgres/src/rows.rs`, `binary_copy.rs`, and `identifiers.rs`;
- `crates/cdf-package-contract/src/provenance.rs`;
- destination mappings in DuckDB, SQLite, ClickHouse, and Postgres;
- project lock/schema compilation and package/evidence tests that preserve Arrow field metadata.

Search noise using “semantic” to mean general behavior or artifact hashing was excluded. The
inventory concerns only Arrow field semantic metadata and decisions derived from it.

## Findings

### Current representation

`cdf-kernel` exposes `cdf:semantic` as an unconstrained `String`. `with_semantic` writes any value
and `semantic` returns it without parsing, validation, registry resolution, or provenance. Arrow
field metadata therefore carries the value through schemas and artifacts, but it does not identify
one immutable definition.

Declarative `FieldDeclaration.semantic: Option<String>` accepts the same free-form value and
`compile_schema` writes it directly. No compile boundary distinguishes descriptive metadata from a
privacy-, fidelity-, control-, or mapping-bearing semantic.

### Behavior-bearing producers and values

| Producer | Current value | Arrow constraint | Current meaning |
|---|---|---|---|
| contract residual/variant capture | `json` | nullable `Utf8` plus residual-encoding metadata | marks the framework `_cdf_variant` column |
| package provenance | `package-row-ord-v1` | non-null `UInt64` plus internal visibility | authenticates `_cdf_package_row_ord` |
| PostgreSQL source catalog/decode | `postgres_json_value_text_v1` | `Utf8` plus `cdf:physical_type=json` | exact JSON text reconstructable as PostgreSQL `JSON` |
| PostgreSQL source catalog/decode | `postgres_jsonb_value_text_v1` | `Utf8` plus `cdf:physical_type=jsonb` | exact JSONB textual value reconstructable through PostgreSQL binary COPY |
| PostgreSQL source catalog/decode | `postgres_numeric_value_text_v1` | `Utf8` plus a validated PostgreSQL NUMERIC declaration | arbitrary-precision NUMERIC text reconstructable losslessly |
| declarative/test schemas | any `pii:*` string | currently any Arrow type | selects the contract PII redaction action |

Tests also exercise a descriptive `id` tag. It has no consumer and therefore has no truthful
definition to preserve. It should be removed from the replacement fixtures rather than promoted
into a meaningless built-in.

`VariantColumnSpec.semantic` is public policy data and is currently configurable even though
`is_framework_variant_field` accepts only the exact `json` constant. That is an invalid latent
state: configuration can author a value that makes the framework-created field cease to be
recognizable as framework-owned.

### Current consumers

1. `cdf-contract::redaction_decision_for_semantic` treats every string beginning `pii:` as PII and
   all other strings as ordinary data. A typo can silently remove privacy behavior.
2. `cdf-contract::is_framework_variant_field` uses exact string equality plus field shape and
   residual encoding to grant framework ownership.
3. `cdf-dest-postgres` matches the three PostgreSQL constants, validates `Utf8` and physical
   provenance, chooses native `JSON`/`JSONB`/`NUMERIC`, selects specialized binary encoders, and
   records the semantic in its load plan. Unknown values fall through to ordinary Arrow mapping.
4. SQLite and ClickHouse call the framework-variant predicate to permit the reserved variant
   column. Other type mapping remains Arrow-only.
5. schema admission compares semantic strings exactly. Contract snapshots, source-plan/schema
   hashes, package schemas, quarantine evidence, and worker artifacts consequently preserve or
   transitively bind the raw string, but none bind a definition hash.

### Architectural seam

The registry belongs below declarative/project compilation and above behavior consumers. A focused
`cdf-semantic` crate can depend on `cdf-kernel`/Arrow/Serde, expose only data models, canonical
resolution, built-in definitions, and compiled facts, and remain free of driver, project, CLI,
filesystem, SQL, or runtime dependencies. `cdf-contract`, `cdf-declarative`, `cdf-project`, and
first-party adapters can depend downward on it without introducing a cycle.

Adapter code must continue to own native encoding and DDL. A semantic definition may select an
adapter-owned mapping profile id and prerequisites; it must not contain SQL or executable adapter
callbacks.

### Direct replacement map

The smallest complete current-format replacement is:

| Current value | Canonical replacement |
|---|---|
| `json` for `_cdf_variant` | `cdf.variant@1` |
| `package-row-ord-v1` | `cdf.package_row_ordinal@1` |
| `pii:<class>` | `cdf.pii@1(class="<class>")` |
| `postgres_json_value_text_v1` | `postgres.json_text@1` |
| `postgres_jsonb_value_text_v1` | `postgres.jsonb_text@1` |
| `postgres_numeric_value_text_v1` | `postgres.numeric_text@1` |
| descriptive `id` and other no-behavior tags | remove, or define explicitly as project semantics after that capability exists |

This is a direct artifact-version transition. No alias table, dual-reader, legacy prefix fallback,
or ignored unknown behavior is justified for a pre-production customer-zero codebase.

## Recommended ratification

### Reference grammar

Use `namespace.name@version` with optional canonical parameters:

```text
cdf.variant@1
cdf.pii@1(class="email")
finance.money@1(currency="USD")
```

- `namespace`, `name`, and parameter keys are lowercase ASCII snake identifiers;
- `version` is an explicit positive `u32` definition version, not a floating “latest” or SemVer
  requirement range;
- parameters are declared by the definition, keys are unique and serialized in lexical order,
  and values are canonical JSON scalars only;
- authoring and artifacts use the same pinned canonical reference; no unversioned aliases;
- an id/version definition is immutable, so changed behavior requires a new version.

### Unknown policy

Every `cdf:semantic` value claims behavior and must resolve. Authored unknowns fail `Contract`;
unapproved source-observed unknowns fail `Data`; a runtime reference absent from its compiled
snapshot is `Internal`. Descriptive metadata belongs in descriptions or a separately named
non-behavior metadata field, not `cdf:semantic`.

### Project definitions

Include project-defined, data-only semantics in lane C after built-ins. They use the same closed
definition schema, Arrow patterns, parameter schema, native validation vocabulary, privacy class,
and destination-profile selectors. They contain no regex source code, SQL, filesystem/network
reference, Python/Wasm callback, or dynamic loader. Explicit project definition files become
hashed project inputs and only reachable definitions enter lock/manifest snapshots.

### Destination selector

Use an exact semantic-reference plus destination-id selector that returns a data-only mapping
profile id, fidelity, allowed Arrow pattern, required metadata predicates, and whether base Arrow
fallback is legal. Most-specific exact semantic mapping wins; equal specificity or conflicting
definitions fail compilation. The destination crate interprets its own profile id and remains
responsible for encoding and verification.

## Conclusions

CDF does not need a second type lattice. It needs a strict definition resolver in front of the
existing Arrow metadata seam and direct migration of six behavior families. The registry must land
before further connector mapping tables multiply magic strings. Built-ins can be implemented before
the project manifest, but final lock/reachable-snapshot publication must coordinate with lane D1.

## Limits

This inventory does not select the SQL annotation syntax, project semantic-definition file format,
or manifest table columns. Those are project-compiler decisions. It does not claim that current
free-form descriptive tags exist in customer projects; it proves only that current code accepts
them.
