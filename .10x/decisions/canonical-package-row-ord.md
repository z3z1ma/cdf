Status: active
Created: 2026-07-18
Updated: 2026-07-18

# Canonical package row ordinal

## Context

CDF's public row address is `(package hash, segment id, zero-based segment row ordinal)`, while relational destinations persist a transactionally allocated compact `_cdf_row_key`. DuckDB and Postgres currently regenerate a row sequence inside each adapter. That duplicates identity work, makes the fastest DuckDB bulk path depend on file ordering or database row enumeration, and forces every future destination to solve the same problem again.

The canonical post-verdict stream already has one deterministic package order after filtering, contract verdicts, quarantine, residual capture, normalization, and package-scoped dedup. Canonical segment assembly is therefore the earliest authority that can assign the persisted output row sequence once for every destination. Unbounded sources still commit bounded package epochs; the row sequence is package-scoped rather than lifetime-scoped.

## Decision

Every canonical Arrow IPC package row carries one framework-owned, non-null `UInt64` field named `_cdf_package_row_ord`. It is zero-based, dense, and strictly increasing in canonical package order. Assignment occurs after all row-selecting and row-reordering operators and when canonical segment boundaries are fixed, before segment encoding and destination staging.

Each manifest segment records its exact `package_row_ord_start`. Its stored ordinal values MUST equal `start..start + row_count`. The package manifest and canonical segment storage schema are current-format-only authority; old artifacts are not read or migrated.

The field is storage/internal evidence, not part of the compiled destination-visible output schema. Destinations receive both the logical output schema and the derived canonical segment schema. Relational adapters compute their transaction-owned compact key as `allocated_package_start + _cdf_package_row_ord`; `_cdf_segments` binds each resulting segment range to package and segment identity. The public segment-local ordinal remains `row_key - segment_range_start`, exactly as before. File/columnar destinations MAY discard the internal field from visible payload when their manifest already provides equivalent physical provenance.

An unbounded source MUST roll finite package epochs according to its compiled checkpoint/flush policy. Segments persist and may enter staged destination ingress incrementally within the epoch; CDF does not retain the whole epoch in memory. Final binding closes the epoch, and the next package resets `_cdf_package_row_ord` to zero under a new package identity.

The framework field has one exact classifier: reserved name, `UInt64`, non-null, `cdf:semantic=package-row-ord-v1`, and `cdf:visibility=internal`. User fields cannot claim it. Memory accounting includes its value buffer, and performance evidence compares package construction and each first-party destination before and after the format change.

## Alternatives considered

- `_cdf_package_row_id`: rejected because the value is an ordinal within one package, not the public row identity or destination key.
- `_cdf_package_row_num`: rejected because SQL row-number vocabulary commonly implies one-based numbering while this evidence is zero-based.
- `_cdf_package_row_ordinal`: superseded because it is needlessly long in every schema and diagnostic; `_ord` retains the precise meaning.
- Assign a segment-local ordinal: rejected because a destination ingesting many segments would still need per-file constants or ordering assumptions to form one compact transaction range.
- Generate ordinals in each destination: rejected because it duplicates universal identity logic and measured DuckDB strategies ranged from 11.95 to 36.76 seconds for the full-year TLC shape.
- Use DuckDB `rowid`: rejected because it is adapter-specific, depends on materialization order, and does not help Postgres or future adapters.
- Persist package/segment strings per row: rejected by the active compact-provenance decision and its measured 5–6x penalty.
- Make the ordinal destination-visible everywhere: rejected because logical output schemas and file destinations must not acquire an implementation column.

## Consequences

Canonical segment hashes and package manifests intentionally change once. DuckDB `read_arrow` can ingest all canonical files in one native scan and derive exact row keys without window functions, sequences, updates, or ordered-file assumptions. Postgres binary COPY and staged merge order consume the same ordinal. Replay verifies the field rather than reconstructing it. Streaming ingestion retains the same proof per finite commit epoch without an ever-growing counter or buffer. The added `UInt64` buffer is a named package overhead that must pass controlled EC2 package and end-to-end gates; failure to meet those gates blocks retention rather than creating an unmeasured default.

This decision supersedes `.10x/decisions/superseded/canonical-package-row-ordinal.md`.

