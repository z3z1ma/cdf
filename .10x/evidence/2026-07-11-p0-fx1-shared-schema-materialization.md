Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/specs/native-format-codec-runtime.md

# Shared physical-to-effective schema materialization

## What was observed

`cdf-contract` now owns physical Arrow batch materialization from the typed `SchemaCoercionPlan`. The operation projects declared outputs, retains zero-copy columns when types already agree, applies Arrow casts for admitted coercions, materializes nullable missing fields, preserves `cdf:source_name` and `cdf:physical_type`, and embeds the exact validated plan as runtime schema evidence.

The engine accepts an unadorned physical batch only when its observed hash and typed plan match the verified effective-schema observation. Source-carried coercion metadata remains rejected unless paired with the existing trusted header. Preview and package execution call the same materializer.

## Procedure

- `cargo test -p cdf-contract shared_coercion_materializer --lib`
- `cargo test -p cdf-engine effective_schema_reuses_observation_across_partitions_and_attests_only_attempted_inputs --lib`
- `cargo test -p cdf-engine package_execution_rejects_ --lib`
- `cargo clippy -p cdf-contract -p cdf-engine --all-targets -- -D warnings`

All commands passed. The effective-schema test supplies physical batches without serialized source coercion headers and still produces the per-observation package artifact.

## What this supports

Physical decoding and shared reconciliation are separate runtime stages. A format driver no longer needs format-local coercion behavior to satisfy engine execution, and the engine remains authoritative over observation identity and verdict evidence.

## Limits

This evidence does not establish accounted ownership for arrays allocated by casts or production registry composition. Those remain within FX1 before the monolithic format path can be deleted.
