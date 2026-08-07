Status: active
Created: 2026-08-06
Updated: 2026-08-06
Supersedes: `.10x/specs/superseded/data-onramp-schema-intelligence-lockfile.md`

# Data-onramp schema intelligence under state authority

## Purpose

This specification governs declared schema, hints, bounded discovery, canonical Arrow versions,
physical reconciliation, coercion, and declarative type vocabulary. Active logical schema
authority is state-backed under `.10x/decisions/state-backed-schema-authority.md`; source
observation never advances it implicitly.

## Authored modes

Resources support authored `declared`, `hints`, and `discover` modes:

- `declared` supplies the logical schema intent directly and validates observations against it;
- `hints` discovers physical reality and applies user fields as constraints/projection;
- `discover` has no authored field list and requires bounded observation for first use.

`schema_mode = "declared|hints|discover"` is explicit. Hints requires a schema block. Discover with
a schema block is invalid. Omitted mode may be resolved from the current authored grammar only if
that grammar's active spec declares the default; this specification does not add a compatibility
fallback.

Before package-producing execution, every mode MUST resolve to one immutable canonical Arrow
schema version and one total admission program. With absent state authority, plan may propose it
and compile/run may establish it. With active authority, authored/current observations are evidence
against that exact version and cannot rewrite it.

## Version and evidence

An immutable schema version contains the complete canonical Arrow schema with field/schema
metadata, content hash, predecessor when promoted, provenance, and optional bounded discovery
evidence reference. State owns versions and active heads. `.cdf/schemas/` may cache verified
canonical copies but is never authority.

Plan/package evidence distinguishes:

- active/proposed logical schema version and head generation;
- compiled output Arrow schema after projection/normalization/framework fields;
- discovery-manifest hash and coverage;
- physical observation hashes/type provenance;
- exact coercion/admission program identity.

Schema and structural fingerprints use the existing canonical recursive encoding, never display
strings or delimiter concatenation.

## Bounded discovery

Discovery is source-specific and bounded:

- Parquet: footer/schema metadata through ranged reads when remote;
- Arrow IPC: schema block;
- CSV/JSON/NDJSON: bounded content sampling, with JSON-family defaults capped at the first 4,096
  records or 8 MiB admitted input;
- SQL: adapter-owned catalogs;
- REST: one bounded sample page plus declared cursor policy.

File resources discover at resource-set grain. Binary formats probe every matched metadata block by
default; explicit sampled file coverage follows
`.10x/specs/schema-discovery-and-stream-admission.md`. Aggregation uses equality or the ratified
lossless recursive widening lattice for the proposed first-use version. Missing compatible fields
become nullable. Incompatible candidates produce complete named verdicts.

Binary metadata defaults remain 64 MiB per file, 128 MiB total in-flight, and eight concurrent
probes per executor, configurable and serialized. Exceeding a bound fails; it never silently
activates sampling or substitutes candidates.

## Reconciliation and drift

Format/source adapters emit observed physical facts into the shared reconciliation stage. Declared
schemas and hints constrain/project/annotate reality; they do not replace observation. Lossless
coercion is compiled and evidenced. Lossy mappings require explicit `allow_lossy_mapping`; parsing
text into semantic temporal/decimal types requires explicit `coerce_types`.

For first use, compatible observations may be aggregated into one proposed baseline. After an
active head exists, no observation may change typed output. It follows
`.10x/specs/schema-drift-dispositions.md`: lossless coerce, typed null, variant, quarantine, or fail.

Row-local mismatches found during bounded JSON-family discovery are proposal evidence. Under active
authority they receive the same compiled run-time disposition as in-stream observations; discovery
does not become permission to admit a new typed field.

## CLI behavior

- `cdf discover source` inventories adapter-owned upstream candidates and may generate resources;
- `cdf discover resource` observes selected authored resources without establishing authority;
- `cdf schema show` reads active state authority;
- `cdf schema diff` performs fresh bounded comparison without writes;
- plan proposes first use without writes;
- compile/run may establish absent authority under their command contracts;
- only `cdf schema promote --execute` advances an established version.

There is no schema pin/discover command and no auto-evolving output mode.

## Type vocabulary

Declarative fields cover the closed current Arrow vocabulary, including decimal128/256 precision
and scale, nested list/struct/map, integer widths, floats, date/time/timestamp/duration, UTF-8/binary
large variants, nullability, and source metadata. Values round-trip through authored grammar,
generated schema, state version, compiled plan, package evidence, and destination mapping.

## Acceptance criteria

- Declared/hints/discover resources resolve to deterministic canonical first-use proposals.
- HTTPS Parquet and REST resources can establish first-use state authority from bounded evidence.
- Multi-file Parquet/IPC aggregation is deterministic and avoids row-data reads.
- Sampling artifacts record both coverage axes and never overclaim exhaustiveness.
- Widening property tests prove value preservation/composition for permitted proposal coercions.
- Decimal/nested types round-trip across every current artifact boundary.
- Active-authority drift never changes the head or destination schema and follows total dispositions.
- Discovery remains executor-neutral and source-specific behavior stays behind adapter contracts.

## Explicit exclusions

- lockfiles or project-file schema authority;
- automatic established-schema widening;
- destination mappings as logical authority;
- unbounded probing, implicit sampling, or source-specific branches in generic orchestration;
- nested promotion or schema export/import.
