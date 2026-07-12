Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/2026-07-11-p3-d2-duckdb-arrow-bulk.md, .10x/tickets/2026-07-11-p3-d3-postgres-binary-copy.md, .10x/tickets/2026-07-11-p3-d4-parquet-streaming-writer.md

# P3 D6: compact provenance across destination archetypes

## Scope

Apply the compact lossless row-provenance model to DuckDB, Postgres, and Parquet/file outputs; expose one logical inspection/correction address; add shared destination conformance and overhead measurements.

## Acceptance criteria

- DuckDB and Postgres payload rows carry one compact allocated row key with exact transactional range mappings; Parquet uses dictionary encoding or a manifest-bound sidecar.
- Logical `(package hash, segment id, row ordinal)` inspection, correction, replay, and diagnostics are identical across destination archetypes.
- No first-party hot path repeats long package/segment strings per payload row when a compact exact representation is available.
- Conformance covers rollback, duplicate package, key-map uniqueness, missing/conflicting maps, correction lookup, and physical-key opacity.
- Provenance overhead is measured and stays within the P3 budget or has a named remaining bottleneck.

## Explicit exclusions

No semantic merge key, truncated-hash identity, compatibility reader, or generic-runtime destination branch.

## Evidence expectations

Before/after bulk and end-to-end profiles, target/storage inspection, cross-adapter conformance, crash/rollback tests, and logical-address round trips.

## References

- `.10x/decisions/compact-lossless-destination-row-provenance.md`
- `.10x/specs/schema-promotion-corrections.md`
- `.10x/specs/destination-bulk-path-runtime.md`

## Progress and notes

- 2026-07-11: DuckDB now implements the canonical compact row-range shape: one allocated payload key and an exact range dimension back to target/package/segment, with logical correction/readback unchanged. Release throughput is 9.42M rows/s versus 11.36M raw. Postgres, Parquet, shared inspection, and cross-adapter conformance remain open. Evidence: `.10x/evidence/2026-07-11-p3-duckdb-compact-row-range-provenance.md`.
