Status: active
Created: 2026-07-08
Updated: 2026-07-10

# Data onramp schema intelligence

## Purpose and scope

This specification governs P2 schema discovery, schema snapshots, hints, declared-schema constraints, reconciliation, widening/coercion, and the declarative Arrow type vocabulary. It refines `.10x/specs/resource-authoring-planning-batches.md` and `.10x/specs/types-contracts-normalization.md` for `VISION.md` Chapters 7, 8, 11, and 19.

## Behavior

Resources MUST support three schema modes: declared, hints, and discover. Discover mode MUST produce a pinned schema snapshot before package-producing execution. Hints mode MUST run discovery and apply user hints as constraints or projection, not as a competing physical truth.

Tier-0 selects an explicit non-default mode with `schema_mode = "declared|hints|discover"`. Omitted mode remains backward compatible: a schema block means declared and no schema block means discover. `hints` requires a schema block, compiles that block's hash as hint authority, discovers physical reality, reconciles it through the shared type policy, and pins the reconciled snapshot while retaining `SchemaSource::Hints` identity. Explicit discover with a schema block is invalid because hints is the unambiguous form.

Schema snapshots MUST be Arrow schemas with metadata, serialized at `.cdf/schemas/<resource>@<hash>.json`, referenced from `cdf.lock`, and stamped into plan/package evidence. Snapshot hashes MUST be deterministic for unchanged source content and unchanged CDF schema serialization.

Discovery probes MUST be bounded and source-specific:

- Parquet: footer/schema metadata through ranged reads when remote.
- Arrow IPC: schema block.
- CSV, JSON, NDJSON: bounded sampling. JSON-family probes MUST stop at the first of 4,096 records or 8 MiB of admitted input by default, record configured and observed coverage, and MUST NOT represent sampled evidence as exhaustive row conformance.
- SQL: catalogs such as `information_schema`.
- REST: one recorded sample page plus declared cursor policy.

File discovery MUST be resource-level rather than single-file-only. A Parquet or Arrow IPC file resource whose glob or remote enumeration resolves multiple files MUST support discovery and pinning without requiring the operator to narrow the source. Per-format footer/schema-block/sampling probes MUST feed one discovery-set aggregation abstraction so later file formats do not reinvent aggregation semantics. The pinned result MUST represent the aggregate resource schema and durable provenance for the matched discovery set; incompatible per-file schemas MUST become named contract verdicts rather than an ambiguity rejection or unclassified crash. `.10x/decisions/multi-file-discovery-aggregation-and-budget.md` governs exhaustive binary aggregation, metadata conflicts, pin/effective/manifest authority, quarantine advancement, and executor budgets.

For Parquet and Arrow IPC, discovery MUST probe every matched footer/schema block by default. Explicit sampled file coverage is permitted only under `.10x/specs/schema-discovery-and-stream-admission.md`; it must never activate implicitly or claim content-exhaustive row conformance. Aggregation MUST use equality or the ratified lossless widening lattice recursively; missing compatible fields become nullable and materialize typed nulls. Initial all-file metadata pinning MUST fail with a complete per-file report when the selected set is incompatible; sampled-file pinning requires the selected sample to aggregate compatibly. Reserved CDF metadata is regenerated; identical non-reserved metadata is retained; conflicts are recorded per file.

Discovery evidence MUST distinguish the immutable baseline snapshot hash, the current verdict-bearing effective schema hash, and the content-addressed discovery-manifest hash. Ordinary commands MUST verify and hydrate the baseline before any file-source observation. Existing pins remain immutable until explicit `cdf schema pin`; `evolve` MAY derive a recorded effective output schema against that baseline, while `freeze` MUST keep the baseline effective and quarantine deviations. File listing/probing for execution MUST NOT be reported or persisted as a pin refresh.

Binary discovery defaults to 64 MiB metadata per file, 128 MiB total in-flight metadata, and 8 concurrent probes per executor. These values MUST be configurable through executor options and serialized into discovery evidence. Exceeding a resolved budget MUST fail explicitly and MUST NOT activate sampling, change an explicit sample, or substitute candidates.

Discovery MUST NOT silently mutate a pinned schema during run. Drift against a pinned snapshot is a contract event that admits, widens, variant-captures, quarantines, or rejects according to policy.

Schema reconciliation MUST be centralized. Format readers MUST feed observed physical schema facts into one reconciliation stage. Declared schemas and hints constrain, project, and annotate observed schema; they do not replace reality.

Automatic widening MUST be lossless and recorded in the validation program. Lossy casts require `allow_lossy_mapping`. String parsing into dates, timestamps, decimals, or other semantic types is opt-in through `coerce_types`; default inference of decimal-looking strings remains `utf8` plus suggestion.

Tier-0 resources MAY declare `types = { coerce_types = <bool>, allow_lossy_mapping = <bool> }`. Both allowances MUST default to `false`, MUST compile into resource runtime policy and the serialized validation program, and MUST apply identically in discovery reconciliation, deep validation, preview, plan, and run.

Row-local mismatches found by sampled JSON-family probes that the governing contract can quarantine MUST be represented as warning verdicts rather than compiler failures. Runtime contract evaluation remains authoritative for all rows and MUST preserve quarantined or residual values as evidence.

Declarative field types MUST cover Arrow's closed vocabulary from `VISION.md` Chapter 7, including decimal128/256 with precision and scale, nested list/struct/map, all integer widths, floats, date/time/timestamp/duration, utf8/binary large variants, and nullability/source metadata.

## CLI/API surface

`cdf schema discover <resource>` MUST probe and print the discovered schema without package or destination writes.

`cdf schema pin <resource>` MUST write or refresh the schema snapshot and render the diff.

`cdf schema show <resource>` MUST render the pinned snapshot.

`cdf schema diff <resource>` MUST compare pinned and newly discovered schemas, including normalizer-version changes where relevant.

`cdf plan` and `cdf run` MAY auto-pin an unpinned discover resource on first use. Auto-pin is a recorded artifact action and MUST be visible in human and JSON output.

## Acceptance criteria

- An HTTPS Parquet resource can plan/run with zero typed schema fields by pinning a footer-discovered snapshot.
- A REST resource without a declared schema can run after bounded plan-time discovery and snapshot pinning.
- The widening lattice has property tests proving value preservation and composition for supported widenings.
- Decimal and nested declarative types round-trip through TOML/YAML parsing, JSON Schema generation, plan evidence, and package schema evidence.
- Physical type provenance is preserved in field metadata after reconciliation.
- Multi-file Parquet and Arrow IPC file discovery pin deterministic aggregate schemas and discovery-set identities without reading row data or narrowing the glob to one file.
- Discovery behavior and artifacts remain executor-neutral and can be reused by standalone, container, or distributed workers without CLI/local-filesystem semantics becoming correctness dependencies.

## Explicit exclusions

This spec does not define file listing, transport credentials, destination type mapping tables, or CLI rendering layout.
