Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-engine-execution-tracing-spans.md, .10x/tickets/2026-07-05-observability-doctor-status-sql.md

# Engine Execution Tracing Spans Evidence

## What Was Observed

`cdf-engine` now preserves the existing `execute_to_package` API and exposes `execute_to_package_with_run_id`, which requires a caller-supplied `RunId`. The traced path emits `cdf_engine.package_execution` spans with exact `run_id`, `resource_id`, and `package_id` fields, plus `cdf_engine.partition_execution` spans with exact `run_id`, `resource_id`, `package_id`, and `partition_id` fields.

The tracing tests capture spans through a local subscriber and compare exact field maps. A traced/untraced package execution comparison proves tracing does not change manifest identity, package hash, or signature. Mutation testing initially found unrelated surviving engine execution assertions; final test hardening killed all viable mutants in `crates/cdf-engine/src/execution.rs`.

## Procedure

Focused final checks:

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo test -p cdf-engine --locked --no-fail-fast`: passed; 10 unit tests and 0 doctests.
- `cargo clippy -p cdf-engine --all-targets --locked -- -D warnings`: passed.
- `cargo mutants --file crates/cdf-engine/src/execution.rs --all-features --cargo-arg --locked --jobs 2 --test-tool cargo --output reports/ai-quality/mutants-engine-tracing -- -p cdf-engine`: passed; 29 mutants tested, 18 caught, 11 unviable, 0 missed.
- `MIRIFLAGS=-Zmiri-disable-isolation CARGO_TARGET_DIR=target/quality/miri-engine-tracing-target cargo +nightly miri test -p cdf-engine traced_execution_emits_run_resource_package_and_partition_spans --locked`: passed; 1 targeted tracing test.
- `CARGO_TARGET_DIR=target/quality/careful-engine-tracing-target cargo +nightly careful test -p cdf-engine --all-features --locked`: passed; 10 unit tests and 0 doctests. The macOS MainThreadChecker dylib warning is a local optional-checker availability warning, not a test failure.

Workspace and feature checks:

- `cargo check --workspace --all-targets --locked`: passed.
- `cargo check --workspace --all-targets --all-features --locked`: passed.
- `cargo check --workspace --all-targets --no-default-features --locked`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed on the final tree.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: passed before the final test-only mutation hardening.
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings`: passed before the final test-only mutation hardening.
- `cargo hack check --workspace --all-targets --each-feature --locked`: passed.
- `cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings`: passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast`: passed on the final tree.
- `cargo test --workspace --all-targets --all-features --locked --no-fail-fast`: passed before the final test-only mutation hardening.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed.
- `cargo nextest run --workspace --locked`: passed on the final tree; 235 tests passed, 0 skipped.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo llvm-cov --workspace --all-features --locked --summary-only`: passed on the final tree; total line coverage 81.24%, `cdf-engine/src/execution.rs` line coverage 90.59%.

Dependency, API, security, and supply-chain checks:

- `cargo tree -p cdf-engine --locked`: confirmed `cdf-engine` has a direct `tracing v0.1.44` dependency.
- `cargo machete`: passed; no unused dependency candidates.
- `CARGO_TARGET_DIR=target/quality/udeps-engine-tracing-target cargo +nightly udeps -p cdf-engine --all-targets --locked`: passed; all deps used.
- `cargo semver-checks --workspace --baseline-rev HEAD`: passed.
- `cargo audit`: passed; 402 dependencies scanned.
- `cargo deny check`: passed. Existing duplicate-version warnings are policy warnings, not failures.
- `cargo vet`: passed; 385 exemptions.
- `osv-scanner scan source -r . --format json --output reports/ai-quality/osv-engine-tracing.json`: passed; `.results | length == 0`.
- `semgrep scan --config p/rust --error --json --output reports/ai-quality/semgrep-rust-engine-tracing-final.json --exclude target --exclude reports .`: passed on the final tree; `.results | length == 0`.
- `gitleaks git --no-banner --redact --report-format json --report-path reports/ai-quality/gitleaks-git-engine-tracing.json .`: passed; 0 findings.
- Clean-source `gitleaks dir --no-banner --redact --report-format json --report-path reports/ai-quality/gitleaks-dir-engine-tracing-final.json <git-clean-snapshot>`: passed on the final tree; 0 findings.
- `tools/codeql-rust-quality.sh`: passed using the persistent database path `target/quality/codeql-db-rust`. The wrapper refreshed the database because the Rust source/manifest/lockfile content fingerprint changed, then analyzed successfully. `target/quality/reports/codeql-rust-current.sarif` has 0 results. CodeQL diagnostics still show the known Rust extractor macro-expansion limits and warning noise recorded in `.10x/knowledge/quality-gate-execution.md`.

Unsafe, maintainability, and duplication checks:

- Direct final source scan `rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|NonNull|UnsafeCell" crates/cdf-engine/src Cargo.lock crates/cdf-engine/Cargo.toml`: no engine source or manifest unsafe markers; only existing transitive crate name `unsafe-libyaml` in `Cargo.lock`.
- `CARGO_TARGET_DIR=target/quality/geiger-engine-tracing-target cargo geiger --manifest-path "$PWD/crates/cdf-engine/Cargo.toml" --all-targets --all-features --include-tests --locked --output-format Json > target/quality/reports/geiger-engine-tracing.json 2> target/quality/reports/geiger-engine-tracing.stderr`: passed. `cdf-engine` used unsafe counts are 0 functions, 0 expressions, 0 unsafe impls, 0 unsafe traits, and 0 unsafe methods.
- `rust-code-analysis-cli -m -p crates/cdf-engine/src -O json --pr` streamed 6 JSON metric documents, wrapped with `jq -s` into `reports/ai-quality/rust-code-analysis-engine-tracing-array.json`.
- `jscpd crates/cdf-engine/src --format rust --reporters console,json --output reports/ai-quality/jscpd-engine-tracing --min-lines 12 --min-tokens 80 --threshold 0 --exit-code 0 --no-colors`: passed; 0 clones, 0 duplicated lines, 0.00%.

## What This Supports

This evidence supports all acceptance criteria for the engine tracing ticket:

- The explicit-run-id API is additive and the existing untraced API remains available.
- Span fields are exact and do not include filters, config, auth material, URLs, environment values, or secret-like plan data.
- The traced path carries manifest identity unchanged compared with the untraced path.
- Quality gates cover focused engine behavior, final workspace behavior, feature/lint/doc/API compatibility, supply chain, secret scanning, unsafe inventory, and mutation resistance for the changed execution file.

## Limits

The CodeQL database was not refreshed a second time after the final mutation-driven test-only hardening, to avoid another expensive database rebuild. The CodeQL run applies to the production implementation and dependency changes; final post-hardening Semgrep, Gitleaks, direct unsafe source scan, workspace tests, nextest, clippy, coverage, `careful`, Miri, and mutation testing all passed on the final tree.

Miri's first default-isolation run failed because `tempfile` needs `mkdir`; the rerun with `-Zmiri-disable-isolation` passed the targeted tracing test. This ticket does not implement `inspect run`, run-id generation, run ledgers, OTLP export, global subscriber setup, package trace artifacts, manifest fields, checkpoint schema, or live run orchestration.
