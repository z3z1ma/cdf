Status: active
Created: 2026-07-08
Updated: 2026-07-09

# Data onramp schema intelligence

## Purpose and scope

This specification governs P2 schema discovery, schema snapshots, hints, declared-schema constraints, reconciliation, widening/coercion, and the declarative Arrow type vocabulary. It refines `.10x/specs/resource-authoring-planning-batches.md` and `.10x/specs/types-contracts-normalization.md` for `VISION.md` Chapters 7, 8, 11, and 19.

## Behavior

Resources MUST support three schema modes: declared, hints, and discover. Discover mode MUST produce a pinned schema snapshot before package-producing execution. Hints mode MUST run discovery and apply user hints as constraints or projection, not as a competing physical truth.

Schema snapshots MUST be Arrow schemas with metadata, serialized at `.cdf/schemas/<resource>@<hash>.json`, referenced from `cdf.lock`, and stamped into plan/package evidence. Snapshot hashes MUST be deterministic for unchanged source content and unchanged CDF schema serialization.

Discovery probes MUST be bounded and source-specific:

- Parquet: footer/schema metadata through ranged reads when remote.
- Arrow IPC: schema block.
- CSV, JSON, NDJSON: bounded sampling.
- SQL: catalogs such as `information_schema`.
- REST: one recorded sample page plus declared cursor policy.

File discovery MUST be resource-level rather than single-file-only. A Parquet or Arrow IPC file resource whose glob or remote enumeration resolves multiple files MUST support discovery and pinning without requiring the operator to narrow the source. Per-format footer/schema-block/sampling probes MUST feed one discovery-set aggregation abstraction so later file formats do not reinvent aggregation semantics. The pinned result MUST represent the aggregate resource schema and durable provenance for the matched discovery set; incompatible per-file schemas MUST become named contract verdicts rather than an ambiguity rejection or unclassified crash. Exact aggregation, large-N sampling, metadata-conflict, and first-pin `freeze` semantics require an explicit ratified contract before implementation.

Discovery MUST NOT silently mutate a pinned schema during run. Drift against a pinned snapshot is a contract event that admits, widens, variant-captures, quarantines, or rejects according to policy.

Schema reconciliation MUST be centralized. Format readers MUST feed observed physical schema facts into one reconciliation stage. Declared schemas and hints constrain, project, and annotate observed schema; they do not replace reality.

Automatic widening MUST be lossless and recorded in the validation program. Lossy casts require `allow_lossy_mapping`. String parsing into dates, timestamps, decimals, or other semantic types is opt-in through `coerce_types`; default inference of decimal-looking strings remains `utf8` plus suggestion.

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

## Explicit exclusions

This spec does not define file listing, transport credentials, destination type mapping tables, or CLI rendering layout.
