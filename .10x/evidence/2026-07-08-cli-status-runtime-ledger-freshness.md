Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-07-cli-status-runtime-ledger-freshness.md, .10x/decisions/status-freshness-authority-precedence.md

# CLI Status Runtime-Ledger Freshness Evidence

## What was observed

`cdf status` now evaluates serving-resource freshness from committed checkpoint heads plus run-ledger/package receipt facts without contacting sources or destinations.

The implementation preserves committed checkpoint head precedence and adds stable JSON receipt-freshness observations for missing run ledger, missing receipt, fresh receipt, stale receipt, and corrupt receipt evidence.

## Procedure

Code changes:

- `crates/cdf-cli/src/status_freshness.rs`
- `crates/cdf-cli/src/tests.rs`
- `.10x/decisions/status-freshness-authority-precedence.md`

Commands run:

```text
cargo fmt --all -- --check
cargo check -p cdf-cli --no-default-features
cargo clippy -p cdf-cli --no-default-features --all-targets -- -D warnings
cargo test -p cdf-cli status_ --no-default-features
cargo test -p cdf-cli --no-default-features
cargo nextest run -p cdf-cli --locked
jscpd crates/cdf-cli/src/status_freshness.rs crates/cdf-cli/src/tests.rs --reporters json,console --output target/quality/reports/jscpd-status-freshness --ignore "**/target/**,**/.git/**,**/reports/**"
jscpd crates/cdf-cli/src/status_freshness.rs --reporters json,console --output target/quality/reports/jscpd-status-freshness-prod-final --ignore "**/target/**,**/.git/**,**/reports/**"
rust-code-analysis-cli -m -O json -p crates/cdf-cli/src/status_freshness.rs > target/quality/reports/rust-code-analysis-status-freshness-final.json
semgrep scan --no-git-ignore --config p/rust --error --json --output target/quality/reports/semgrep-status-freshness-final.json crates/cdf-cli/src/status_freshness.rs crates/cdf-cli/src/tests.rs
gitleaks detect --no-git --source crates/cdf-cli/src --report-format json --report-path target/quality/reports/gitleaks-status-freshness.json --no-banner --redact
rg -n "unsafe|extern \"|\*const|\*mut|impl Send|impl Sync" crates/cdf-cli/src/status_freshness.rs crates/cdf-cli/src/tests.rs
cargo deny check
cargo audit --json > target/quality/reports/cargo-audit-status-freshness.json
cargo vet --locked
cargo machete
osv-scanner scan source --lockfile Cargo.lock --format json --output-file target/quality/reports/osv-status-freshness.json
cargo semver-checks --package cdf-cli
cargo geiger --manifest-path "$PWD/crates/cdf-cli/Cargo.toml" --all-targets --all-features --include-tests --locked --output-format Json
tools/codeql-rust-quality.sh
jq '[.runs[].results // [] | length] | add' target/quality/reports/codeql-rust-current.sarif
```

## Results

Passing gates:

- `cargo fmt --all -- --check`: passed.
- `cargo check -p cdf-cli --no-default-features`: passed.
- `cargo clippy -p cdf-cli --no-default-features --all-targets -- -D warnings`: passed.
- `cargo test -p cdf-cli status_ --no-default-features`: passed; 16 status/resume-filtered tests passed.
- `cargo test -p cdf-cli --no-default-features`: passed; 115 unit tests plus 1 integration test passed.
- `cargo nextest run -p cdf-cli --locked`: passed; 116 tests passed, 0 skipped.
- `semgrep p/rust`: passed; 2 files scanned, 11 rules, 0 findings.
- `gitleaks detect --no-git --source crates/cdf-cli/src`: passed; no leaks found.
- Direct touched-file unsafe/FFI/raw-pointer/Send/Sync search: no matches.
- `cargo deny check`: passed; final summary `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo audit --json`: passed; 0 vulnerabilities.
- `cargo vet --locked`: passed; `Vetting Succeeded (452 exempted)`.
- `cargo machete`: passed; no unused dependency candidates.
- `tools/codeql-rust-quality.sh`: passed through the reusable database wrapper. The wrapper refreshed `target/quality/codeql-db-rust` because Rust source/manifest/lockfile fingerprint changed. SARIF result count was `0`.

Status-specific test evidence:

- Missing state DB remains non-evaluable and does not create a state DB.
- Missing checkpoint table remains non-evaluable.
- Missing run ledger with no committed head reports `run_ledger_missing` and `receipt_freshness.state = missing_run_ledger`.
- A committed head with no run ledger remains fresh/stale by checkpoint timestamp and reports `receipt_freshness.state = missing_run_ledger`.
- Receipt-only run-ledger facts with package receipt artifacts report `fresh_receipt` or `stale_receipt` and exit 0 or 1 respectively.
- Receipt-only run-ledger facts without a matching package receipt artifact report `receipt_missing` and exit 78.
- A stale package receipt timestamp does not override a fresh committed checkpoint timestamp; the resource stays fresh and the receipt observation reports `corrupt_receipt`.

Metrics and non-blocking tool outcomes:

- `jscpd` over `status_freshness.rs` only: 1 file, 873 lines, 2 clones, 14 duplicated lines, 1.60% duplicated lines, 2.35% duplicated tokens.
- `jscpd` over `status_freshness.rs` plus `tests.rs`: 2 files, 6,399 lines, 90 clones, 867 duplicated lines, 13.55% duplicated lines. The high percentage is dominated by existing integration-test fixture/assertion repetition in `tests.rs`; production-only duplication is low.
- `rust-code-analysis-cli` production hotspots: `evaluate_resource` cyclomatic 13/cognitive 3, `missing_receipt_observation` cyclomatic 12/cognitive 2, `committed_head_receipt_freshness` cyclomatic 10/cognitive 3, `matching_package_receipt` cyclomatic 7/cognitive 7.
- `osv-scanner` exited 1 for the already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` advisory only; no additional OSV findings were present.
- `cargo semver-checks --package cdf-cli` is not applicable because `cdf-cli` is private/publish=false and not found in crates.io.
- `cargo geiger` was started against `crates/cdf-cli/Cargo.toml` but the session stopped responding and was interrupted. This matches the project knowledge that geiger is noisy/costly in this workspace. The direct touched-file unsafe scan found no first-party unsafe/FFI/raw-pointer/Send/Sync surface in this slice.
- CodeQL retained the known local Rust extractor warning profile: 229 Rust files extracted, 176 with extraction warnings/errors according to CodeQL metrics, 53 without, 3,515 extraction warnings, 0 extraction errors, 0 SARIF findings.

## What this supports or challenges

This supports closing `.10x/tickets/done/2026-07-07-cli-status-runtime-ledger-freshness.md`.

It supports that `cdf status` now has scheduler-readable runtime-ledger/package-receipt freshness states while preserving the checkpoint commit gate as the state-advancement authority.

## Limits

The tests use local SQLite/package fixtures and do not contact live sources or destinations. This is intentional for `cdf status`.

The receipt-only fresh/stale path is a status observation path, not state advancement; recovery remains governed by the run-ledger and checkpoint specifications.
