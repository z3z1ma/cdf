Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Compact lossless destination row provenance

## Context

CDF's logical row address is `(package hash, segment id, row ordinal)`. The first relational implementation repeated the package hash and segment id as VARCHAR values on every destination row. On the TLC shape, raw DuckDB Arrow append measured 10.4–11.3M rows/s while the exact provenance-bearing path measured only 1.75–1.95M rows/s. Repeating long immutable identifiers consumed most destination time and made the correctness feature cost roughly 5–6x.

The logical address must remain homogeneous and lossless across destinations. Requiring one wasteful physical encoding everywhere would confuse interface consistency with storage layout and violate the P3 overhead budget.

## Decision

`RowProvenanceAddress` remains the destination-neutral public and correction address. Every targetable destination MUST resolve it exactly and expose it through the same inspection/correction interfaces.

At scale, physical persistence uses compact dictionary keys. Relational destinations store one transactionally allocated row key on each payload row. A framework-owned dimension binds disjoint contiguous key ranges to the full target, package hash, and segment id; subtracting the range start yields the exact segment row ordinal. Keys are never truncated hashes, and exact identifiers remain authority. Columnar/file destinations use dictionary encoding or a manifest-bound provenance sidecar with the same exact mapping.

First-party adapters expose the same logical address and inspection model. CLI inspection renders logical identifiers, never implementation keys, unless an explicit physical/explain view is requested. The physical encoding is adapter-owned bulk-path work. Generic runtime passes only logical package/segment/row authority and MUST NOT branch on destination identity.

## Alternatives considered

- Repeat full strings per row: rejected by the measured 5–6x DuckDB penalty and similar wire/storage amplification for Postgres.
- Truncate the package hash into a deterministic 64-bit key: rejected because birthday collisions become material at long-horizon scale and collision handling would infect replay.
- Remove row provenance: rejected because correction, residual promotion, and exact replay evidence depend on it.
- Let every destination invent unrelated layouts: rejected because operators need one logical address and one inspection model.

## Consequences

The correction contract requires the logical tuple plus an exact compact-key mapping rather than literal strings on every relational row. Existing pre-production target layouts are not migrated or read; replace rebuilds them under the current-format-only policy. Destination conformance proves round-trip resolution, uniqueness, rollback, replay, and correction behavior. Performance evidence includes provenance overhead versus the same bulk path without provenance.
