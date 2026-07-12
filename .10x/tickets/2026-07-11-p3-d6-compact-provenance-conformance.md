Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-11-p3-d2-duckdb-arrow-bulk.md, .10x/tickets/done/2026-07-11-p3-d3-postgres-binary-copy.md, .10x/tickets/2026-07-11-p3-d4-parquet-streaming-writer.md

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
- 2026-07-11: Postgres now implements the same logical/physical split. Payload rows carry only `_cdf_row_key BIGINT`; a transactionally locked allocator reserves contiguous segment ranges and `_cdf_segments` binds each exact range to target/package/segment. Residual readback and correction resolve the canonical logical tuple through the range map. The superseded `_cdf_load`/`_cdf_segment`/`_cdf_row` payload layout and tests were deleted. All 30 active unit/live tests pass. Equal-shape local binary COPY improved to 1.898M rows/s and 3.11x its CSV control. Parquet/file provenance and the shared cross-adapter matrix remain open. Evidence: `.10x/evidence/2026-07-11-p3-d6-postgres-compact-row-range-provenance.md`.
- 2026-07-11: Parquet/file output now uses the immutable object manifest as its compact provenance index. The same canonical manifest bytes are published under a target/package provenance key and are receipt-verified against the package-token manifest. Logical addresses resolve to object key plus row ordinal; correction planning fails before publication when any address is missing or outside the segment row range. No provenance column or per-row identifier was added to Parquet payloads. All 27 active adapter tests pass and strict all-target/all-feature clippy is clean. The remaining D6 work is the generated cross-adapter logical-address/crash matrix and aggregate closure evidence. Evidence: `.10x/evidence/2026-07-11-p3-d6-parquet-manifest-provenance.md`.
