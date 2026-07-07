Status: recorded
Created: 2026-07-06
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-06-status-freshness-local-ledger.md, .10x/specs/project-cli-observability-security.md, .10x/specs/checkpoint-state-commit-gate.md

# Status freshness local ledger evidence

## What was observed

`cdf status` now evaluates compiled resources with `trust_level = serving` and a `FreshnessSpec` against the configured local SQLite checkpoint database using read-only inspection. The evaluator lives in `crates/cdf-cli/src/status_freshness.rs`, keeping `crates/cdf-cli/src/lib.rs` as a small module index and avoiding a monolithic CLI implementation.

The implementation reports:

- `fresh` when exactly one committed local checkpoint head exists for `resource_id + state_scope` and `age_ms <= max_age_ms`.
- `stale` when exactly one committed head exists and `age_ms > max_age_ms`.
- `non_evaluable` when the state database is missing, the checkpoint table is missing, no committed head exists, or more than one pipeline has a matching committed head.

It does not create missing state databases and does not invent a pipeline default.

## Procedure

Focused post-hardening checks:

- `cargo fmt --all -- --check` passed.
- `cargo test -p cdf-cli --locked --no-fail-fast` passed: 43 CLI unit tests, 1 integration test, 0 doctests.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings` passed.
- `cargo nextest run -p cdf-cli --locked` passed: 44 tests run, 44 passed.
- `cargo mutants -p cdf-cli --file crates/cdf-cli/src/status_freshness.rs --re "human_summary|age_ms" --test-tool nextest --jobs 2 --timeout 900 --baseline skip --cargo-arg=--locked` passed: 14 mutants tested, 14 caught.

Mutation-test learning:

- An initial focused mutation pass exposed surviving `human_summary` mutants; non-JSON status assertions were added for no-SLO, fresh, stale, and non-evaluable summaries.
- A follow-up focused pass exposed a surviving `age_ms` subtraction mutant; an elapsed-age assertion for a checkpoint committed two minutes earlier was added.
- A future-committed checkpoint assertion was also added for the clock-skew clamp contract.

Workspace and feature-matrix gates observed earlier in the same workstream before the final test-only hardening:

- `cargo check --workspace --all-targets --locked` passed.
- `cargo check --workspace --all-targets --all-features --locked` passed.
- `cargo check --workspace --all-targets --no-default-features --locked` passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` passed.
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings` passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast` passed.
- `cargo test --workspace --all-targets --all-features --locked --no-fail-fast` passed.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast` passed.
- `cargo nextest run --workspace --locked` passed: 202 tests run, 202 passed.
- `cargo hack check --workspace --all-targets --each-feature --locked` passed.
- `cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings` passed.
- `cargo llvm-cov --workspace --all-features --locked --summary-only` passed: total line coverage 79.77%; `crates/cdf-cli/src/status_freshness.rs` line coverage 91.42%.
- `cargo doc --workspace --all-features --no-deps --locked` passed.
- `cargo semver-checks --workspace --baseline-rev HEAD` passed.

Supply-chain, security, and metrics checks:

- `cargo machete` passed with no unused dependencies found.
- `cargo audit --json` passed with 0 vulnerabilities and 0 warnings.
- `cargo deny check advisories` passed.
- `cargo deny check` passed: advisories, bans, licenses, and sources all ok.
- `cargo vet` passed: 385 exemptions.
- `osv-scanner scan source -r . --format json` passed with 0 vulnerabilities.
- Final `semgrep scan --config p/rust --json --error --exclude target --exclude .git .` passed with 0 findings and 0 errors.
- Final `semgrep scan --config p/security-audit --json --error --exclude target --exclude .git .` passed with 0 findings and 0 errors.
- `gitleaks git --no-banner --redact` passed with 0 findings.
- Final `gitleaks dir` over a source snapshot excluding `.git`, `target`, and `reports` passed with 0 findings.
- Final `tools/codeql-rust-quality.sh` passed with 0 SARIF findings. The reusable database stayed at `target/quality/codeql-db-rust`; it was refreshed because Rust source changed after mutation-hardening tests were added. CodeQL reported 0 extraction errors and 1660 extraction warnings, consistent with the known local Rust extractor macro limitations recorded in `.10x/knowledge/quality-gate-execution.md`.
- First-party unsafe marker search over `crates/` found no `unsafe` blocks/impls/traits, FFI markers, raw pointer conversion markers, `transmute`, or `MaybeUninit`.
- `cargo geiger` with isolated `CARGO_TARGET_DIR=target/quality/geiger-status-target` completed and exited nonzero because dependency crates produced 192 unsafe warnings. This was paired with the first-party unsafe marker search above.
- `cargo +nightly udeps --workspace --all-targets --locked` passed.
- `cargo +nightly careful test -p cdf-cli --locked` passed: 41 unit tests, 1 integration test, 0 doctests at the time of that run.
- `cargo bloat --release -p cdf-cli --bin cdf-cli -n 20` passed; the top symbols were dominated by DuckDB/libduckdb, with `cdf_cli::commands::dispatch` the first cdf-owned symbol shown.
- `rust-code-analysis-cli -m -p crates -O json` passed and wrote JSON metrics under `target/quality/reports/rust-code-analysis-status-freshness/`.
- `jscpd` passed as a metric run and reported 4.10% duplicated lines overall; no status-freshness-specific blocker was identified.
- `cargo fuzz list` returned no fuzz manifest at `fuzz/Cargo.toml`; there are no repository fuzz targets to run for this slice.
- `cargo +nightly miri test -p cdf-cli --locked status_` was installed and attempted. With default isolation it stopped on `SystemTime::now`; with `MIRIFLAGS=-Zmiri-disable-isolation`, the no-SQLite status test passed and the run then stopped on unsupported macOS Miri foreign function `sqlite3_threadsafe` in `rusqlite`. This is a Miri/native-FFI coverage limit, not evidence of a product defect.
- `kani` / `cargo-kani` was not installed and no Kani harness exists in the repository.

## What this supports

The evidence supports closing `.10x/tickets/done/2026-07-06-status-freshness-local-ledger.md`: the CLI status contract is implemented, tested through JSON and human output paths, mutation-hardened around the summary and age arithmetic edges, and rechecked against the required quality suite where applicable.

## Limits

The final source change after the broad workspace gates was test-only hardening. Full workspace gates were not repeated after those final assertions, but package-level fmt, test, clippy, nextest, focused mutation testing, final Semgrep, final gitleaks source scan, final first-party unsafe search, and final CodeQL were rerun on the final tree.

CodeQL and Miri limits are tool/extractor limits documented above. Geiger's nonzero result is dependency unsafe exposure; first-party source did not contain unsafe/FFI/raw-pointer markers.
