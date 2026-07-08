Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-source-decode-type-drift-quarantine-seam.md, .10x/decisions/source-decode-type-drift-quarantine.md, .10x/decisions/contract-live-verdict-execution-semantics.md

# Source Decode Pre-Contract Kernel/Engine Slice

## What Was Observed

The passive kernel fact shape and engine package-folding slice compile and pass focused verification.

`cdf-engine` focused package execution with one manually constructed pre-contract source-decode fact:

- writes one normal accepted data segment;
- writes one package quarantine record under `quarantine/part-000001.parquet`;
- carries `error_code = "source_type_mismatch"`;
- carries `rule_id = "source-decode:event_type:type-mismatch"`;
- merges the fact into `stats/verdict-summary.json` and `stats/quarantine-summary.json`.

`cdf-kernel` header serde compatibility defaults a missing legacy `pre_contract_quarantine` field to an empty vector and skips empty vectors when serializing.

## Procedure

- `cargo fmt --all -- --check`
- `cargo test --locked -p cdf-engine source_decode_quarantine_facts_fold_into_package_artifacts -- --nocapture`
- `cargo test --locked -p cdf-kernel batch_header_serde_defaults_missing_pre_contract_quarantine -- --nocapture`
- `cargo check --locked -p cdf-kernel -p cdf-engine --all-targets`
- `cargo clippy --locked -p cdf-kernel -p cdf-engine --all-targets -- -D warnings`

All commands completed successfully on 2026-07-08.

## What This Supports

This supports the narrow implementation slice where source/runtime-owned pre-contract quarantine facts can be carried on `BatchHeader` and folded by `cdf-engine` into package quarantine and summary artifacts without weakening `cdf-contract::evaluate_record_batch`.

## Limits

This evidence does not cover declared-schema NDJSON decoding, malformed-input fail-closed behavior, destination quarantine mirrors, checkpoint gating, or the E6 conformance fixture. Those remain owned by the active ticket's broader scope and were intentionally not implemented in this slice.
