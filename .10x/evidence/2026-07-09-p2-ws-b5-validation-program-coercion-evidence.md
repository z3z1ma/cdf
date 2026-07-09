Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-b5-validation-program-coercion-evidence.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/decisions/data-onramp-schema-discovery-reconciliation.md

# P2 WS-B5 validation-program coercion evidence

## What Was Observed

B5 adds backwards-compatible validation-program schema coercion evidence and package artifact serialization for reconciled schemas that carry `cdf:physical_type` provenance.

`ValidationProgram` now has optional `schema_coercion` evidence with serde defaults, so legacy JSON lacking the field still deserializes. `cdf-engine` writes the initial validation program at package start as before, then enriches and rewrites `plan/validation-program.json` on successful package completion when reconciled output schema metadata exposes physical provenance. When coercion evidence exists, the package also writes `schema/coercion-plan.json`. `schema/output.json` now carries `cdf:physical_type` and paired `cdf:source_name` metadata only for fields where physical provenance exists, avoiding broad output-schema artifact churn for ordinary normalized fields.

Focused tests prove `Int32 -> Int64` evidence is classified as `widened`, names observed and constraint types, preserves an unchanged `Utf8` field as `preserved`, and writes matching plan/schema package evidence.

## Procedure

- `CARGO_TARGET_DIR=target/codex-b5 cargo test -p cdf-contract schema_coercion --locked`
  - Result: passed.
  - Coverage: focused reconciled-schema coercion evidence extraction test passed.
- `CARGO_TARGET_DIR=target/codex-b5 cargo test -p cdf-contract validation_program_coercion --locked`
  - Result: passed.
  - Coverage: optional validation-program coercion evidence serde compatibility test passed.
- `CARGO_TARGET_DIR=target/codex-b5 cargo test -p cdf-engine package_artifacts_record_schema_coercion_evidence_and_physical_type_metadata --locked`
  - Result: passed.
  - Coverage: package artifacts contain enriched validation-program coercion evidence, dedicated `schema/coercion-plan.json`, `widened` `Int32 -> Int64` decision, preserved field decision, and output schema `cdf:physical_type` metadata.
- `CARGO_TARGET_DIR=target/codex-b5 cargo test -p cdf-contract --locked`
  - Result: passed.
  - Coverage: 31 unit tests passed; doc tests ran 0 tests and passed.
- `CARGO_TARGET_DIR=target/codex-b5 cargo test -p cdf-engine --locked`
  - Result: passed.
  - Coverage: 30 unit tests passed; doc tests ran 0 tests and passed.
- `CARGO_TARGET_DIR=target/codex-b5 cargo clippy -p cdf-contract -p cdf-engine --all-targets --locked -- -D warnings`
  - Result: passed.
- `cargo fmt --all -- --check`
  - Result: passed.
- `git diff --check -- crates/cdf-contract/src/compiler.rs crates/cdf-contract/src/program.rs crates/cdf-contract/src/reconciliation.rs crates/cdf-contract/src/tests.rs crates/cdf-engine/src/execution.rs crates/cdf-engine/src/tests.rs .10x/tickets/2026-07-09-p2-ws-b5-validation-program-coercion-evidence.md`
  - Result: passed.
- `jscpd --format rust --min-lines 8 --min-tokens 80 --reporters console,json --output target/codex-b5/quality/jscpd-p2-b5 --ignore "**/target/**,**/.git/**,**/reports/**" crates/cdf-contract/src/compiler.rs crates/cdf-contract/src/program.rs crates/cdf-contract/src/reconciliation.rs crates/cdf-contract/src/tests.rs crates/cdf-engine/src/execution.rs crates/cdf-engine/src/tests.rs`
  - Result: exited 0.
  - Output summary: 6 files analyzed; 14 clone blocks; 182 duplicated lines; 1890 duplicated tokens; JSON report at `target/codex-b5/quality/jscpd-p2-b5/jscpd-report.json`.
  - Inspection: reported clone ranges are in existing-style test setup and repeated assertion/helper patterns across large test files. No new implementation abstraction was introduced because the B5 production diff is small and the repeated test setup is clearer local to the assertions.
- `rust-code-analysis-cli -m -O json -p crates/cdf-contract/src/reconciliation.rs > target/codex-b5/quality/rust-code-analysis-p2-b5/reconciliation.json`
  - Result: completed.
- `rust-code-analysis-cli -m -O json -p crates/cdf-engine/src/execution.rs > target/codex-b5/quality/rust-code-analysis-p2-b5/execution.json`
  - Result: completed.

## What This Supports Or Challenges

This supports the B5 acceptance criteria for optional validation-program coercion evidence, structured package evidence, widened observed/constraint type naming, physical provenance in package schema evidence, deterministic evidence derived from schema metadata rather than clocks or host paths, and preservation of ordinary package behavior when no `cdf:physical_type` provenance exists.

## Limits

The test does not re-run the dirty `cdf-formats`/`cdf-declarative` local Parquet reader tree because concurrent D4 work owns those files. It uses the reconciled schema shape established by B3 evidence: a materialized `Int64` output field carrying `cdf:physical_type = Int32`, plus an unchanged field. B3 remains the evidence owner for the actual declared Parquet reader materialization path.

An initial accidental parallel Cargo run against the shared `target/` directory collided with build fingerprints. All usable verification above was rerun with `CARGO_TARGET_DIR=target/codex-b5` to avoid disturbing concurrent workers.
