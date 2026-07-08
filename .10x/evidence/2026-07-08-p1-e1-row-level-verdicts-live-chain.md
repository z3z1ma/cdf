Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-e1-row-level-verdicts-live-chain.md, .10x/specs/types-contracts-normalization.md, .10x/specs/package-lifecycle-determinism.md, .10x/decisions/contract-live-verdict-execution-semantics.md

# P1 E1 row-level verdicts in the live chain

## What was observed

`ValidationProgram` now serializes executable row rule programs for nullability, domain, range, regex, freshness, and dedup metadata. `cdf-contract` exposes a pure Arrow evaluator that returns an accepted-row Boolean selection, quarantine candidates, and a verdict summary from `ValidationProgram` plus `ContractEvaluationContext`.

`cdf-engine` calls that evaluator from the live `ContractExec` path before normalization. Accepted rows continue to `NormalizeExec`; reject-batch and reject-run dispositions abort with contract errors before package finalization and destination mutation. Packages still serialize `plan/validation-program.json`; freshness packages also serialize `plan/contract-evaluation-context.json` with package-level `observed_at_ms`.

Parent review found that a fully quarantined batch could leave a zero-row data segment in the package path. The live engine now skips post-contract empty batches before normalization/package segment writing, and the freshness test asserts that all-rejected input produces no data segments.

## Procedure

Parent-observed review and verification used these commands:

```text
cargo check -p cdf-contract -p cdf-engine --all-targets --locked
cargo test -p cdf-contract --locked
cargo test -p cdf-engine --locked
cargo test -p cdf-conformance conformance_local_contract_evaluator_owns_row_verdict_path --locked
cargo test -p cdf-contract local_non_public_type_null_domain_100k_rows_benchmarkable_path --locked -- --nocapture
cargo fmt --all -- --check
cargo clippy -p cdf-contract -p cdf-engine --all-targets --locked -- -D warnings
cargo nextest run -p cdf-contract -p cdf-engine --locked
jscpd --reporters console --no-tips crates/cdf-contract/src/program.rs crates/cdf-contract/src/compiler.rs crates/cdf-contract/src/evaluator.rs crates/cdf-contract/src/lib.rs crates/cdf-contract/src/tests.rs crates/cdf-engine/src/execution.rs crates/cdf-engine/src/tests.rs crates/cdf-conformance/src/property_fuzz/contract.rs
rust-code-analysis-cli -m -p crates/cdf-contract/src/evaluator.rs -O json -o target/quality/reports/rust-code-analysis-p1-e1-contract
rust-code-analysis-cli -m -p crates/cdf-engine/src/execution.rs -O json -o target/quality/reports/rust-code-analysis-p1-e1-engine
rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|Unpin|Send|Sync" crates/cdf-contract/src/program.rs crates/cdf-contract/src/compiler.rs crates/cdf-contract/src/evaluator.rs crates/cdf-contract/src/lib.rs crates/cdf-contract/src/tests.rs crates/cdf-engine/src/execution.rs crates/cdf-engine/src/tests.rs crates/cdf-conformance/src/property_fuzz/contract.rs
cargo vet --locked
cargo audit --deny warnings --ignore RUSTSEC-2024-0436
cargo deny check
osv-scanner --lockfile Cargo.lock --format json > target/quality/reports/osv-p1-e1-row-level-verdicts.json
cargo machete --with-metadata crates/cdf-contract > target/quality/reports/cargo-machete-p1-e1-cdf-contract.txt
cargo machete --with-metadata crates/cdf-engine > target/quality/reports/cargo-machete-p1-e1-cdf-engine.txt
gitleaks dir --redact --no-banner --report-format json --report-path target/quality/reports/gitleaks-p1-e1-cdf-contract.json crates/cdf-contract
gitleaks dir --redact --no-banner --report-format json --report-path target/quality/reports/gitleaks-p1-e1-cdf-engine.json crates/cdf-engine
gitleaks dir --redact --no-banner --report-format json --report-path target/quality/reports/gitleaks-p1-e1-cdf-conformance.json crates/cdf-conformance
semgrep scan --config p/rust --error --json --no-git-ignore --output target/quality/reports/semgrep-p1-e1-row-level-verdicts.json crates/cdf-contract/src/program.rs crates/cdf-contract/src/compiler.rs crates/cdf-contract/src/evaluator.rs crates/cdf-contract/src/lib.rs crates/cdf-contract/src/tests.rs crates/cdf-engine/src/execution.rs crates/cdf-engine/src/tests.rs crates/cdf-conformance/src/property_fuzz/contract.rs
tools/codeql-rust-quality.sh
git diff --check
```

## Results

- `cargo fmt --all -- --check`: pass.
- `cargo check -p cdf-contract -p cdf-engine --all-targets --locked`: pass.
- `cargo clippy -p cdf-contract -p cdf-engine --all-targets --locked -- -D warnings`: pass.
- `cargo test -p cdf-contract --locked`: pass; 15 tests passed.
- `cargo test -p cdf-engine --locked`: pass; 20 tests passed.
- `cargo test -p cdf-conformance conformance_local_contract_evaluator_owns_row_verdict_path --locked`: pass.
- `cargo nextest run -p cdf-contract -p cdf-engine --locked`: pass; 35 tests passed.
- Local/non-public throughput path: `cargo test -p cdf-contract local_non_public_type_null_domain_100k_rows_benchmarkable_path --locked -- --nocapture` passed and printed `local_non_public_contract_eval_type_null_domain rows=100000 elapsed_ms=15.180`.
- `jscpd`: exit 0; 8 files analyzed, 3514 lines, 24085 tokens, 15 clones, 127 duplicated lines (3.61%), 1066 duplicated tokens (4.43%). The clones were concentrated in focused test/setup repetition and typed Arrow branch patterns; no abstraction was added because the repetition did not hide a shared invariant.
- `rust-code-analysis-cli -m -p crates/cdf-contract/src/evaluator.rs`: exit 0. Visible metrics included evaluator unit SLOC 670, PLOC 627, 42 functions, average cyclomatic complexity about 3.83; `evaluate_record_batch` cognitive sum 10 and cyclomatic sum 12.
- `rust-code-analysis-cli -m -p crates/cdf-engine/src/execution.rs`: exit 0. Visible metrics included execution unit SLOC 357, PLOC 324, average cognitive complexity 1.00, average cyclomatic complexity 2.96; `execute_to_package_inner` cognitive sum 17 and cyclomatic sum 34.
- Direct unsafe/FFI/raw-pointer scan over touched Rust files: no matches.
- `cargo vet --locked`: pass; `Vetting Succeeded (402 exempted)`.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: pass after scanning 447 locked crate dependencies.
- `cargo deny check`: pass; emitted the repo's existing Arrow 58/59 duplicate warnings, then ended with `advisories ok, bans ok, licenses ok, sources ok`.
- `osv-scanner --lockfile Cargo.lock`: nonzero only for the already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` advisory.
- `cargo machete --with-metadata crates/cdf-contract`: pass for the touched crate.
- `cargo machete --with-metadata crates/cdf-engine`: pass for the touched crate.
- `gitleaks dir` over `crates/cdf-contract`, `crates/cdf-engine`, and `crates/cdf-conformance`: pass; no leaks found.
- `semgrep scan --config p/rust` over touched Rust files: pass; 8 files scanned, 11 Rust rules, 0 findings.
- `tools/codeql-rust-quality.sh`: pass. The reusable `target/quality/codeql-db-rust` database was refreshed because the Rust source/manifest/lock fingerprint changed; SARIF result count was 0. Diagnostics reported 213/213 Rust files scanned, extraction errors 0, extraction warnings 3230, files extracted total 213, files extracted with errors 162, files extracted without errors 51, macro calls resolved 115/4520. The high macro diagnostic count is the existing CodeQL Rust extractor limitation pattern, not a query finding.
- `git diff --check`: pass.

## Dependency and supply-chain note

`cdf-contract` now directly depends on `arrow-array 59.1.0`, `regex 1.12.4`, and `sha2 0.10.9`. `Cargo.lock` changed only to list those already-present locked packages under `cdf-contract`; no new package versions were introduced.

## What this supports

- The compiled program contains row-rule metadata sufficient for runtime evaluation without the original `ContractPolicy`.
- The evaluator fails closed for missing program coverage, batch type mismatches, missing explicit row-rule columns, malformed regex/range literals, missing freshness `observed_at_ms`, and incompatible freshness timestamp columns.
- Quarantine dispositions produce accepted-row selection, per-violation candidates with rule id, error code, source row ordinal, source position, and redacted observed value, while keeping package quarantine artifact writing outside E1.
- Reject-batch and reject-run dispositions abort the live path before packaged manifests are finalized.
- Freshness uses package-level `observed_at_ms` and writes the context artifact only for programs that require freshness.

## Limits

- The throughput output is explicitly local and non-public. It is a benchmarkable test path, not a public performance claim.
- Dedup is serialized as metadata for this child. Live merge/dedup enforcement remains outside E1 and is owned by `.10x/tickets/2026-07-08-p1-e3-merge-dedup-live-path.md`.
- Quarantine artifact writing and destination quarantine mirrors remain outside E1 and are owned by `.10x/tickets/2026-07-08-p1-e2-quarantine-routing-redaction.md`.
