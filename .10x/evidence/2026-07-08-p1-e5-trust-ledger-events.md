Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-e5-trust-ring-ledger-events.md, .10x/decisions/contract-anomaly-signal-demotion-policy.md

# P1 E5 trust-ring ledger events evidence

## What was observed

P1 E5 now records trust-ring validation-depth transitions as run-ledger evidence:

- `new_resource`: first contact records `discovery -> full`;
- `clean_stable_runs`: stable clean committed schema history records `full -> sampled_fast_path` after the compiled threshold;
- `drift`: stable promoted resources demote `sampled_fast_path -> full` when the schema hash changes and `demote_on_drift` is set;
- `quarantine_event`: stable promoted resources demote on package quarantine artifacts when `demote_on_quarantine` is set;
- `anomaly_spike`: stable promoted resources demote only when `demote_on_anomaly` is set and an explicit anomaly fact is present.

An explicit anomaly fact has fields `metric`, `observed`, `threshold`, and `window`. E5 does not infer anomaly spikes from row counts, quarantine counts, destination failures, elapsed time, or other heuristics.

Run-ledger transition events carry resource/package/run context through the common ledger fields, checkpoint/package pointers where available, depth fields, trigger, schema hash, and anomaly fact fields for `anomaly_spike`. Checkpoint state still advances only through the receipt-gated checkpoint store.

## Procedure

Behavior/build checks:

- `cargo fmt --all -- --check`
- `git diff --check`
- `cargo check -p cdf-project -p cdf-contract -p cdf-cli --all-targets --locked`
- `cargo test -p cdf-project trust_ring_ --locked -- --nocapture`
- `cargo test -p cdf-cli inspect_run_reports_completed_run_json_and_human --locked -- --nocapture`
- `cargo test -p cdf-contract validation_program_serializes_and_has_total_lattice --locked`
- `cargo test -p cdf-contract trust_presets_expand_to_specified_policy_shapes --locked`
- `cargo test -p cdf-conformance property_fuzz --locked`
- `cargo check --workspace --all-targets --locked`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`

Quality and scanner checks:

- `jscpd crates/cdf-contract/src crates/cdf-project/src/runtime crates/cdf-project/src/runtime_tests.rs crates/cdf-conformance/src/property_fuzz/contract.rs --reporters json,console --output target/quality/reports/jscpd-p1-e5-trust-ledger --ignore "**/target/**,**/.git/**,**/reports/**"`
- `rust-code-analysis-cli -m -O json -p crates/cdf-contract/src -p crates/cdf-project/src/runtime -p crates/cdf-project/src/runtime_tests.rs -p crates/cdf-conformance/src/property_fuzz/contract.rs > target/quality/reports/rust-code-analysis-p1-e5-trust-ledger.json`
- `scc --format json crates/cdf-contract/src crates/cdf-project/src/runtime crates/cdf-project/src/runtime_tests.rs crates/cdf-conformance/src/property_fuzz/contract.rs > target/quality/reports/scc-p1-e5-trust-ledger.json`
- `rg -n "\bunsafe\b|extern \"|raw pointer|\*const|\*mut|unsafe impl|impl Send|impl Sync" crates/cdf-contract/src crates/cdf-project/src/runtime crates/cdf-project/src/runtime_tests.rs crates/cdf-conformance/src/property_fuzz/contract.rs > target/quality/reports/unsafe-rg-p1-e5-trust-ledger.txt || true`
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-p1-e5-trust-ledger.json crates/cdf-contract/src crates/cdf-project/src crates/cdf-cli/src crates/cdf-conformance/src/property_fuzz/contract.rs`
- `semgrep scan --config p/security-audit --error --json --output target/quality/reports/semgrep-security-p1-e5-trust-ledger.json crates/cdf-contract/src crates/cdf-project/src crates/cdf-cli/src crates/cdf-conformance/src/property_fuzz/contract.rs`
- `gitleaks detect --no-git --source crates --report-format json --report-path target/quality/reports/gitleaks-p1-e5-trust-ledger.json --no-banner --redact`
- `cargo audit --json > target/quality/reports/cargo-audit-p1-e5-trust-ledger.json`
- `cargo deny check > target/quality/reports/cargo-deny-p1-e5-trust-ledger.txt 2>&1`
- `cargo vet --locked > target/quality/reports/cargo-vet-p1-e5-trust-ledger.txt 2>&1`
- `osv-scanner scan source --lockfile Cargo.lock --format json > target/quality/reports/osv-p1-e5-trust-ledger.json`
- `tools/codeql-rust-quality.sh 2>&1 | tee target/quality/reports/codeql-p1-e5-trust-ledger.log`

## Results

- Formatting and diff hygiene: passed.
- Focused project trust-ring tests: passed, 5 tests.
- CLI inspect-run regression: passed.
- Focused contract tests: passed.
- Conformance property/fuzz slice: passed, 16 tests.
- Full workspace check: passed.
- Full workspace clippy: passed with `-D warnings`.
- `jscpd`: completed; 67 clones, 679 duplicated lines, 6.23% duplicated lines, `newClones = 0`, `newDuplicatedLines = 0`. A small trust-ring request helper reduced new local repetition; remaining duplication is pre-existing broad test harness repetition.
- `rust-code-analysis-cli`: completed and wrote `target/quality/reports/rust-code-analysis-p1-e5-trust-ledger.json`.
- `scc`: completed and wrote `target/quality/reports/scc-p1-e5-trust-ledger.json`.
- Direct unsafe/FFI/raw-pointer scan: no matches.
- Semgrep Rust profile: passed with 0 findings.
- Semgrep security-audit profile: passed with 0 findings.
- Gitleaks source scan: passed with no leaks.
- `cargo audit`: passed with 0 vulnerabilities.
- `cargo deny check`: passed; report ends with `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo vet --locked`: passed; report contains `Vetting Succeeded (424 exempted)`.
- OSV: exited nonzero only for the already-ratified `paste` advisory `RUSTSEC-2024-0436`.
- CodeQL: first run caught a real workspace build gap in `cdf-conformance` after `ValidationProgram` gained the explicit anomaly field; the gap was repaired. The rerun refreshed the reusable database because the prior failed run left no fingerprint, completed successfully, and `target/quality/reports/codeql-rust-current.sarif` contains 0 results. Known local extractor limits remain: 224 Rust files extracted, 0 extraction errors, 3344 extraction warnings, and 4740 unresolved macro calls.

## What this supports

This supports closing P1 E5. Trust-ring promotion and demotion events are recorded as run-ledger evidence, include explicit anomaly-fact demotion without inferred heuristics, remain redaction-guarded through existing ledger event detail handling, and do not bypass the receipt-gated checkpoint store.

## Limits

No production anomaly detector or `ProfileExec` anomaly producer was added. The current implementation provides the explicit fact shape and runtime/ledger semantics required for E5; future profiling/anomaly work can emit the same fact shape under a separate ticket.
