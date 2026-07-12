Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a6-spillable-package-dedup.md

# Dedup consumes final package output rows

## What was observed

The package dedup barrier now receives rows only after residual JSON materialization, identifier normalization, effective-schema canonicalization, and compiled output-schema conformance. Exact-row compilation includes the framework residual variant field when the contract emits one. The evaluator resolves that final-output field without weakening ordinary validation-program coverage.

Two rows with identical typed columns but distinct `_cdf_variant` values are retained as distinct exact rows. Existing append exact-row package behavior remains green.

## Procedure

- `cargo test -p cdf-contract exact_row_dedup_compares_the_final_residual_variant_field -- --nocapture` — passed.
- `cargo test -p cdf-engine append_exact_row_dedup_compiles_and_drops_only_complete_duplicates -- --nocapture` — passed.
- `cargo check -p cdf-contract -p cdf-engine` — passed after removing stale pre-output plumbing.

## What this supports

This closes the semantic-placement defect identified by the A6 audit: exact-row identity is the complete normalized package row, including captured nonconformant values, rather than an intermediate accepted batch.

## Limits

The evaluator still retains package-wide payload, key groups, masks, and inline provenance. The spillable barrier and v2 bounded provenance remain active A6 work.
