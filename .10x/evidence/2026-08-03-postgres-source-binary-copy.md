Status: recorded
Created: 2026-08-03
Updated: 2026-08-03

# PostgreSQL source binary COPY evidence

## Observation

The PostgreSQL table source now emits bounded Arrow batches from canonical binary COPY OUT without
`postgres::Row` or floating-point NUMERIC conversion. PostgreSQL 17 live coverage preserves the
approved JSON/JSONB, UUID, Decimal128/256, and tagged exact-text domains. The original source-child
roofline passed at `1.027722x` fixed-width and `1.192054x` mixed. After parent closure repairs, the
larger 500,000-row/five-sample rerun remained above the required floor at `0.955591x` and
`1.227744x` respectively.

## Procedure

- `cargo test -p cdf-source-postgres --lib -j 12` — 20 passed, one explicitly live test ignored.
- `TEST_DATABASE_URL=postgresql://cdf@127.0.0.1:55440/postgres CARGO_BUILD_JOBS=12 cargo test -p
  cdf-source-postgres live_binary_copy_preserves_json_uuid_and_exact_numeric_domains -j 12 --
  --ignored --nocapture` — one passed on PostgreSQL `17.10`.
- `DUCKDB_DOWNLOAD_LIB=1 TEST_DATABASE_URL=postgresql://cdf@127.0.0.1:55440/postgres
  CARGO_BUILD_JOBS=12 cargo test -p cdf-project
  general_project_run_executes_postgres_table_resource_stream -j 12 -- --nocapture` — one passed,
  270 filtered out.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-postgres --tests -p cdf-benchmarks --bin
  postgres-source-roofline -j 12 -- -D warnings` — passed.
- `DUCKDB_DOWNLOAD_LIB=1
  CDF_POSTGRES_SOURCE_URL=postgresql://cdf@127.0.0.1:55440/postgres
  CDF_POSTGRES_SOURCE_ROOFLINE_SAMPLES=3 CDF_POSTGRES_SOURCE_ROOFLINE_ROWS=250000 cargo run
  --release -p cdf-benchmarks --bin postgres-source-roofline` — passed both cells.

The roofline report is
`.10x/evidence/.storage/2026-08-03-postgres-source-roofline.json`. It records PostgreSQL `17.10`,
250,000 rows per sample, three alternating measured samples after untimed warmups, exact useful
Arrow bytes and content checksums, CPU/RSS, four 65,536-row-or-smaller batches, medians/MAD, host
identity, workspace input hash, and executable hash.

Parent closure revalidation added seven focused decoder/memory/display-scale tests, two error-owner
tests, one UInt64 numeric-order regression, the existing live binary-source regression, strict
source Clippy, and the final two-cell roofline. The raw final report is
`.10x/evidence/.storage/2026-08-03-postgres-source-roofline-final.json`; the complete integration and
repair evidence is `.10x/evidence/2026-08-03-postgres-source-destination-integration.md`.

## What it supports or challenges

- Supports exact canonical text for UUID, JSON, JSONB, unconstrained NUMERIC, precision above 76,
  and scale above Arrow precision; JSON and JSONB retain distinct semantic tags.
- Supports exact Decimal128/256 decoding, negative scale, the padded 20-group precision-76 edge,
  NULL, `NaN` fail-before-publication with an explicit `Utf8` remedy, and both infinities under the
  tagged text mapping.
- Supports preservation of projection/order/limit/cursor and managed execution through the focused
  project test, plus malformed-stream, fragmentation, trailer/EOF, nullability, and memory laws
  through unit coverage.
- Supports numeric UInt64 ordering before text transfer, preallocation-free wide schemas,
  allocation-safe fragmented near-limit text, PostgreSQL NUMERIC display-scale validation, and
  typed server/transport error ownership.
- Supports the ratified `>=0.90` throughput floor. Fixed-width medians were 44,183,542 ns CDF and
  45,408,416 ns direct; mixed medians were 79,480,625 ns CDF and 94,745,250 ns direct.

## Limits

The performance evidence is a warm loopback Apple Silicon/PostgreSQL 17 cell, not a remote-network
claim. The synchronous PostgreSQL client does not expose physical protocol-byte counters, so the
report records them as unavailable rather than estimating them. Cross-connection range
partitioning, CDC, arbitrary SQL resources, and destination reconstruction remain outside this
source ticket; the destination behavior is owned by the next child ticket.
