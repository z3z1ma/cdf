Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-ws-a11-hints-schema-mode.md

# P2 A11 Hints schema mode evidence

## Observation

Tier-0 accepts explicit `schema_mode = "hints"`, requires declared constraints, discovers the observed physical schema through the ordinary bounded probe, reconciles it with the shared type policy, preserves discovery-manifest metadata, and pins a `SchemaSource::Hints` snapshot. A physical Parquet `int32` constrained by an `int64` hint runs with an `int64` package field.

Explicit `discover` plus a schema block and `hints` without a schema fail during compilation with targeted remediation.

## Procedure

- `cargo test -p cdf-declarative -p cdf-project -p cdf-cli`: passed (268 CLI unit tests, 169 project/runtime tests, 98 declarative tests, plus integration/doc tests).
- `cargo test -p cdf-declarative schema_mode_`: 2 focused declaration tests passed.
- `cargo test -p cdf-cli hints_schema_discovers_pins_and_constrains_observed_parquet`: focused plan/pin/run test passed.
- `cargo clippy -p cdf-declarative -p cdf-project -p cdf-cli --all-targets -- -D warnings`: passed.

## Limits

This evidence exercises lossless width reconciliation. Other incompatibilities retain the already-covered shared reconciliation policy; A11 adds no alternate coercion lattice.
