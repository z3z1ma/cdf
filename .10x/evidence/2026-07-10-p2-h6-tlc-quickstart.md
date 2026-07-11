Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-ws-h6-tlc-quickstart.md

# P2 H6 TLC quickstart evidence

## Observation

`docs/quickstart.md` now uses the public NYC TLC yellow-taxi Parquet dataset as the canonical onboarding path. It distinguishes bounded footer discovery from data loading, retains one logical partition per month, explains FileManifest no-op/new-file behavior, describes typed drift quarantine and reviewed repinning, and replays a verified package without source contact.

## Procedure

- `cargo test -p cdf-conformance p2_ --locked`: all 9 matrix/conformance tests passed.
- `cargo test -p cdf-cli p2_s1_add_http_parquet_pins_and_runs_with_zero_typed_fields --locked`: passed.
- `cargo test -p cdf-cli p2_s2_http_month_glob_is_incremental_and_no_change_is_a_noop --locked`: passed.
- `cargo test -p cdf-cli governed_evolve_quarantines_incompatible_file_with_exact_arrow_field_evidence --locked`: passed.
- Every repository-relative link in the changed docs resolves by inspection.

## Limits

The live public CDN returned 403 for data GETs during the earlier recorded session even though footer add/discovery succeeded; the quickstart names upstream denial and provides deterministic equivalents rather than inventing a live success claim.
