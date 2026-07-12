Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-d6-compact-provenance-conformance.md, .10x/decisions/compact-lossless-destination-row-provenance.md, .10x/specs/schema-promotion-corrections.md

# Parquet manifest-bound row provenance

## What was observed

Parquet payload objects remain pure destination data: no package hash, segment id, or row-address columns are repeated per row. The adapter publishes the already-required canonical object manifest under an immutable target/package provenance key. Each manifest object binds one segment id, exact row count, content hash, and object key, so the shared logical `(package hash, segment id, row ordinal)` address resolves losslessly to `(object key, row ordinal)`.

The receipt records the provenance-manifest key and verification requires its bytes to equal the package-token manifest exactly. Duplicate recovery recreates or verifies the alias before accepting a prior manifest. Correction planning deduplicates requested addresses and rejects a missing package, missing segment, or ordinal outside the recorded row count before writing correction data.

## Procedure

- `cargo test -p cdf-dest-parquet --lib`
  - 27 passed, 0 failed, 1 release roofline benchmark ignored.
- `cargo clippy -p cdf-dest-parquet --all-targets --all-features -- -D warnings`
  - passed.
- Focused adapter tests commit a two-row base object, resolve row zero to its exact object/ordinal, reject the first out-of-range ordinal, exercise correction publication and abort against a real committed base, verify duplicate recovery, and reject tampered receipt evidence.
- The reusable destination correction conformance assertion now requires Parquet to declare persisted and targetable provenance as supported.

## What this supports or challenges

This supports the Parquet/file slice of D6 without introducing a destination-specific logical contract. Relational destinations use compact allocated row-key ranges; Parquet uses its immutable segment/object manifest. All expose the kernel-owned logical address, while physical storage remains adapter-owned.

The added hot-path payload cost is zero bytes and zero columns per row. Metadata cost is one additional immutable copy of the canonical manifest per target/package, scaling with segments rather than rows. Publication occurs outside row encoding and is included in receipt verification.

## Limits

This evidence covers the Parquet adapter slice. D6 remains open for one generated cross-adapter matrix covering rollback, duplicate packages, missing/conflicting physical maps, logical correction lookup, and physical-key opacity across DuckDB, Postgres, and Parquet.
