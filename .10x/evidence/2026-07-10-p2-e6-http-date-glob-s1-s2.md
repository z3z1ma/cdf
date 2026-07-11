Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-ws-e6-http-date-glob-and-s1-s2-conformance.md, .10x/tickets/2026-07-08-p2-ws-i-conformance-parity-friction-suite.md

# P2 E6 HTTP date glob and S1/S2 evidence

## What was observed

The file transport now distinguishes required metadata from optional candidate metadata. Only an HTTP 404 becomes `None`; authorization, rate-limit, transient, and other request failures retain their typed errors. A canonical filename with one year-qualified month wildcard expands to 01–12, and absent candidates are skipped. Other arbitrary HTTP wildcards remain rejected because HTTP has no LIST.

Deterministic S1 performs `cdf add` against a ranged HTTP Parquet fixture, pins the discovered normalized schema with zero typed fields, plans, runs, writes a package, commits DuckDB, verifies receipt/checkpoint evidence, and preserves source names. Deterministic S2 plans and previews two present months from the 12 finite candidates, loads four rows, performs a fast unchanged no-op without a package, then observes a newly present third month and loads only its two rows; DuckDB contains six rows total.

## Procedure

- `cargo test -p cdf-declarative -p cdf-project`: 96 and 169 tests passed.
- `cargo test -p cdf-cli`: 266 unit tests plus the doctor environment integration test passed.
- `cargo test -p cdf-conformance p2_`: 9 P2 matrix/conformance tests passed.
- `cargo clippy -p cdf-declarative -p cdf-project -p cdf-cli -p cdf-conformance --all-targets -- -D warnings`: passed.
- Focused guards passed: `file_transport_http_optional_metadata_treats_only_404_as_absent`, `http_numeric_template_expands_finitely_and_preserves_width`, `http_year_month_glob_skips_absent_candidates_without_hiding_other_failures`, `p2_s1_add_http_parquet_pins_and_runs_with_zero_typed_fields`, and `p2_s2_http_month_glob_is_incremental_and_no_change_is_a_noop`.

## Live TLC observation

At approximately 2026-07-10 19:33 America/Phoenix, `HEAD https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet` returned 200 with content length 49,961,641, ETag, and byte-range advertisement. `cdf add tlc.yellow <URL>` completed in about 0.3 seconds, inferred and pinned all 19 fields under schema hash `sha256:916e8470a951fafa0c48851ef4ac1fca5d5312c6717fe1ee687cae59f3d245b9`, and wrote no typed schema block. The subsequent run failed before package completion when the upstream returned GET 403. Independent curl requests for both `Range: bytes=0-1023` and ordinary GET returned the same CloudFront 403 at that time, including with a browser user agent. This is external availability evidence, not a CDF semantic failure or a reason to weaken auth/error classification.

## Limits

The deterministic fixtures prove CDF behavior without depending on a public service. A successful full public-data GET remains desirable release/demo evidence when the endpoint permits it; the failed live attempt is reproducible and preserved here.
