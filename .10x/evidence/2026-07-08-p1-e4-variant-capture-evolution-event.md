Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-e4-variant-capture-evolution-event.md, .10x/tickets/done/2026-07-08-p1-contract-depth-program.md

# P1 E4 variant capture and contract-evolution evidence

## What Was Observed

P1 E4 adds live `_cdf_variant` capture in `cdf-engine` for compiled `NestedAction::CaptureVariant` columns. Struct, List, LargeList, and Map values are materialized into deterministic canonical JSON strings in a single semantic `json` variant column; the source nested columns are removed from the normalized output batch. Unsupported Arrow types and non-finite floats fail closed through contract errors.

Package evidence now writes `schema/contract-evolution.json` when the validation program contains variant capture. The artifact lists captured source fields in deterministic order, records the target variant column and semantic tag, records an empty `promotion_events` list, and records `implicit_promotion_count: 0`. Promotion is therefore evidence-backed but not implicit.

The parent review split variant capture out of `crates/cdf-engine/src/execution.rs` into `crates/cdf-engine/src/variant_capture.rs`. The new hotspot was reduced from the initial `arrow_value_to_json` cyclomatic complexity of 39 to a final maximum cyclomatic complexity of 18 in `integer_value_to_json`; `normalize_batch` is the final cognitive hotspot at cyclomatic 13, cognitive 15, and SLOC 59. The existing `execute_to_package_inner` remains the broader engine hotspot at cyclomatic 41, cognitive 26, and SLOC 163, unchanged by this closure.

## Procedure

Focused behavior checks:

- `cargo test -p cdf-engine --locked --no-fail-fast`: passed, 28 tests.
- `cargo test -p cdf-conformance property_fuzz::contract --locked -- --nocapture`: passed, 4 tests.
- `cargo test -p cdf-engine variant_capture_materializes_nested_values_and_contract_evolution_evidence --locked -- --nocapture`: passed.
- `cargo nextest run -p cdf-engine -p cdf-conformance --locked`: passed, 90 tests, 0 skipped.

Final quality gates:

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo check -p cdf-engine -p cdf-conformance --all-targets --locked`: passed.
- `cargo clippy -p cdf-engine -p cdf-conformance --all-targets --locked -- -D warnings`: passed.
- `jscpd crates/cdf-engine/src crates/cdf-conformance/src/property_fuzz --reporters json,console --output target/quality/reports/jscpd-p1-e4-variant-capture-final --ignore "**/target/**,**/.git/**,**/reports/**"`: passed. Reported 12 files, 4,503 lines, 31,309 tokens, 26 clones, 237 duplicated lines (5.26%), 2,443 duplicated tokens (7.80%), `newClones=0`, `newDuplicatedLines=0`.
- `rust-code-analysis-cli -m -O json -p crates/cdf-engine/src > target/quality/reports/rust-code-analysis-p1-e4-engine-final.json`: passed.
- `rust-code-analysis-cli -m -O json -p crates/cdf-engine/src/variant_capture.rs > target/quality/reports/rust-code-analysis-p1-e4-variant-capture-after.json`: passed and supplied the final complexity numbers above.
- `cargo machete --with-metadata --skip-target-dir crates/cdf-engine crates/cdf-conformance`: passed.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-p1-e4-variant-capture-final.json crates/cdf-engine/src crates/cdf-conformance/src/property_fuzz`: passed, 0 findings.
- `semgrep scan --config p/security-audit --error --json --output target/quality/reports/semgrep-security-p1-e4-variant-capture-final.json crates/cdf-engine/src crates/cdf-conformance/src/property_fuzz`: passed, 0 findings.
- `gitleaks detect --no-git --source crates --report-format json --report-path target/quality/reports/gitleaks-p1-e4-variant-capture-final.json --no-banner --redact`: passed, no leaks.
- `cargo deny check`: passed with the known duplicate Arrow 58/59 warnings already covered by P0 Workstream D.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436 --json > target/quality/reports/cargo-audit-p1-e4-variant-capture.json`: passed.
- `cargo vet --locked > target/quality/reports/cargo-vet-p1-e4-variant-capture.txt`: passed with `Vetting Succeeded (402 exempted)`.
- `osv-scanner scan source --lockfile Cargo.lock --format json --output-file target/quality/reports/osv-scanner-p1-e4-variant-capture.json`: reported only the already-ratified `RUSTSEC-2024-0436` advisory for `paste` 1.0.15.
- `cargo tree -p cdf-engine --locked > target/quality/reports/cargo-tree-p1-e4-cdf-engine.txt`: passed.
- `cargo tree -p cdf-conformance --locked > target/quality/reports/cargo-tree-p1-e4-cdf-conformance.txt`: passed.
- `tools/codeql-rust-quality.sh`: passed using the reusable database path `target/quality/codeql-db-rust`; `target/quality/reports/codeql-rust-current.sarif` contains 0 results. The final run refreshed the reusable database because the source fingerprint changed.

## What This Supports

The engine behavior test `variant_capture_materializes_nested_values_and_contract_evolution_evidence` supports the E4 acceptance criteria:

- `_cdf_variant` is materialized with semantic tag `json`.
- Struct/List/Map fields are preserved as deterministic canonical JSON rather than silently dropped.
- Captured nested columns are removed from normalized output.
- `schema/output.json` records the variant semantic tag.
- `schema/contract-evolution.json` is canonical JSON, included in package verification, and records zero implicit promotions.
- `PackageReader::verify()` and `replay_view()` see the contract-evolution artifact and segment.
- Quarantine redaction still hashes a PII observed value in a run that also captures variants, and the quarantine Parquet artifact does not contain the raw secret string.

The conformance test `conformance_nested_unknown_fields_compile_to_variant_capture` supports compiler-level coverage: an Experimental trust policy with nested unknown structure compiles to `NestedAction::CaptureVariant { column_name: "_cdf_variant", semantic: "json" }`.

## Limits

This evidence does not claim destination-specific variant type mapping, child-table expansion, arbitrary JSON schema inference, or trust-ring promotion/demotion ledger events. Those are explicitly excluded from E4 or owned by E5/E6. The OSV gate remains non-clean only for the already-ratified `paste` advisory exception.
