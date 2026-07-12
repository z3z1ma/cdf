Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-d6-compact-provenance-conformance.md, .10x/decisions/compact-lossless-destination-row-provenance.md, .10x/specs/schema-promotion-corrections.md

# Postgres compact row-range provenance

## Observation

Postgres payload tables now persist one `_cdf_row_key BIGINT` plus the existing load timestamp. `_cdf_row_key_allocator` atomically reserves contiguous ranges inside the package transaction. `_cdf_segments` stores the exclusive key range with exact target, package hash, and segment id. The public logical address remains `(package hash, segment id, row ordinal)`; the physical key is never accepted as correction or merge identity.

The superseded three-column payload representation was deleted without a compatibility reader or migration. Merge ordering uses the monotone row key. Append, replace, merge, duplicate replay, abort rollback, exact residual readback, addressed correction, missing/duplicate address rejection, and post-update rollback passed against live ephemeral PostgreSQL.

## Performance

On the Apple M5 Pro local PostgreSQL release control, 524,288 TLC-shaped rows measured 1,898,152 binary COPY rows/s versus 610,026 rows/s for equal-shape CSV COPY, a 3.11x speedup. The preceding binary implementation with repeated package/segment strings measured 1,662,005 rows/s, so compact provenance improved the server-inclusive binary path by 14.2%. The in-memory equal-shape encoder measured 26,038,425 binary rows/s versus 10,877,137 CSV rows/s (2.39x), with 16,777,229 versus 18,819,489 encoded bytes.

## Verification

- `cargo test -p cdf-dest-postgres --lib`: 30 passed, two release benchmarks ignored.
- `cargo test --release -p cdf-dest-postgres binary_copy_encoder_is_at_least_twice_csv -- --ignored --nocapture`: passed at 2.39x.
- `cargo test --release -p cdf-dest-postgres live_binary_copy_is_at_least_twice_csv -- --ignored --nocapture`: passed at 3.11x.
- `cargo clippy -p cdf-dest-postgres --all-targets -- -D warnings`: passed.

## Limits

This proves the relational Postgres implementation and its logical correction/readback behavior. Parquet/file sidecar or dictionary persistence and the generated cross-adapter conformance matrix remain owned by D6.
