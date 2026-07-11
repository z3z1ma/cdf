Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-rp10-residual-promotion-conformance.md, .10x/specs/residual-variant-capture.md, .10x/specs/schema-promotion-corrections.md

# RP10 residual capture and promotion conformance

## What was observed

Residual capture and promotion are now exercised as source- and destination-neutral laws:

- NDJSON/JSON fixtures cover clean values, unknown fields, scalar mismatch, nested variance, malformed framing, PII-safe evidence, control-field quarantine, and unsupported residual encoding.
- Parquet and Arrow IPC fixtures explicitly preserve extra physical values as row-addressed residual candidates outside the pinned projection.
- A sampled two-of-three-file pin encounters an unseen `/score` field at runtime. Preview and run both use the pinned constraint, report/capture two residual-bearing rows with zero row quarantine, and do not mutate during preview. Fresh exhaustive discovery generates identical repeated promotion plans. DuckDB addressed correction materializes both values and clears every residual.
- The shared destination correction conformance law validates declared provenance, targetability, readback, and strategy capabilities for DuckDB, Postgres, and Parquet. Command scenarios then prove DuckDB/Postgres in-place update and Parquet immutable sidecar execution.
- RP9's crash matrix, lease/CAS fencing, exact multi-target chain, later-target source-free recovery, and stale-run checkpoint guard prove the pin cannot advance before exact correction checkpoints.
- RP9D's shared availability matrix covers retained, missing, malformed, inconsistent-receipt, tombstone-only, and would-remove-last-promotable-copy classifications without inferring destination readback.
- Canonical codec properties and package verification cover exact residual round-trip; deterministic snapshot/package/plan tests bind golden identity and repeated planning.

## Procedure

- `cargo test --workspace`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo fmt --all -- --check`
- `git diff --check`
- Focused suites during closure: `cdf-engine` 50 passed; `cdf-project` 163 passed; `cdf-state-sqlite` 38 passed; `cdf-dest-postgres` 40 passed; CLI preview 23 passed; CLI promotion 10 passed.

Primary regression owners:

- `sampled_pin_captures_unseen_field_then_fresh_discovery_promotes_without_source_replay`
- `declared_parquet_projection_preserves_extra_fields_as_residual_candidates`
- `declared_arrow_ipc_projection_preserves_extra_fields_as_residual_candidates`
- `declared_ndjson_all_rows_mismatch_and_unknown_variance_decode_to_residual_candidates`
- `residual_contract_exec_captures_safe_values_redacts_pii_and_quarantines_controls`
- `variant_capture_materializes_nested_values_and_contract_evolution_evidence`
- `residual_unsupported_encoding_becomes_named_quarantine`
- RP9C command/crash tests named in `.10x/evidence/2026-07-10-p2-rp9c-promotion-command-conformance.md`
- RP9D availability tests named in `.10x/evidence/2026-07-10-p2-rp9d-gc-promotion-availability.md`

## What this supports

This supports every RP10 acceptance criterion and closes the residual capture/promotion child program without claiming that unrelated P2 network/cloud golden paths are complete.

## Limits

S1-S4/S6/S8 final program status remains governed by `.10x/tickets/2026-07-08-p2-ws-i-conformance-parity-friction-suite.md`; RP10 adds its exact tests to the matrix but does not promote those scenario rows.
