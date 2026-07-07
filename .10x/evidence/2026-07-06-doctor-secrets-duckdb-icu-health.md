Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-doctor-secrets-duckdb-icu-health.md, .10x/specs/project-cli-observability-security.md

# Doctor Secrets and DuckDB ICU Health Evidence

## What was observed

`cdf doctor --json` now reports structured project health details, redacted secret-resolution details, and DuckDB ICU probe details for the scoped CLI doctor surface.

Observed implementation state:

- `project_file.details` includes `project_root`, `selected_environment`, `compiled_resources`, and `lockfile_present`.
- Successful `secrets.details` includes only `count` and `secret://...` references.
- Resolved environment, file, declarative auth-token, and declarative SQL connection secret references are covered without mutating the parent test process environment.
- Missing or unavailable secrets make `doctor` exit nonzero without structured success details or resolved secret values.
- A later missing SQL secret does not leak already resolved destination, file, or auth-token secret values.
- Missing DuckDB database files produce a skipped `duckdb_icu` check without creating the database.
- Existing DuckDB database files run the ICU sort-key probe and report safe diagnostic details without assuming the local ICU extension outcome.

## Procedure

Reviewed the scoped diff in `crates/cdf-cli/src/commands.rs`, `crates/cdf-cli/src/tests.rs`, `crates/cdf-cli/tests/doctor_env.rs`, `.10x/specs/project-cli-observability-security.md`, and `.10x/tickets/done/2026-07-06-doctor-secrets-duckdb-icu-health.md`.

Commands and results:

- `cargo fmt --all -- --check`: passed.
- `git diff --check -- . ':(exclude).gitignore'`: passed.
- `cargo test -p cdf-cli --locked --no-fail-fast`: passed, including 35 unit tests and the `doctor_env` integration test.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed during worker and parent loops.
- `cargo test --workspace --all-targets --locked --no-fail-fast`: passed, including the new `doctor_env` integration test.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo nextest run --workspace --locked`: passed, 186 tests run.
- `cargo test --doc --workspace --locked`: passed.
- `cargo doc --workspace --no-deps --locked`: passed.
- `cargo hack check --workspace --feature-powerset --locked`: passed across 17 workspace package feature sets.
- `CARGO_TARGET_DIR=target/quality/semver-target cargo semver-checks --workspace --baseline-rev HEAD`: passed for all workspace crates.
- `cargo llvm-cov --workspace --locked --summary-only`: passed, 186 tests under instrumentation; total line coverage 78.90%, `cdf-cli/src/commands.rs` line coverage 59.69%.
- `cargo machete`: passed, no unused dependencies reported.
- `cargo mutants -p cdf-cli --file crates/cdf-cli/src/commands.rs --in-diff /tmp/cdf-doctor-secrets-icu-commands.diff --test-package cdf-cli --jobs 4 --timeout 120 --output target/quality/reports/mutants-doctor-secrets-icu -- --locked`: passed, 6 mutants tested, 4 caught, 2 unviable, 0 missed, 0 timed out.
- `semgrep scan --config p/rust --error --no-git-ignore --json --output target/quality/reports/semgrep-rust-doctor-secrets-icu.json --exclude target --exclude reports --exclude .git .`: passed, 0 results. An explicit follow-up scan of `crates/cdf-cli/tests/doctor_env.rs` passed with 0 results after replacing `std::env::temp_dir()` with a workspace-local `target/quality/test-projects` fixture root.
- `tools/codeql-rust-quality.sh`: passed using reusable `target/quality/codeql-db-rust`; SARIF result count 0; fingerprint file size 65 bytes. Final run scanned 129 Rust files. Rust extractor diagnostics still reported macro extraction warnings, but no query findings.
- `osv-scanner scan --format json --output-file target/quality/reports/osv-doctor-secrets-icu.json .`: passed, 0 results.
- `cargo audit --json > target/quality/reports/cargo-audit-doctor-secrets-icu.json`: passed, `found=false`, `count=0`.
- `gitleaks dir <temporary source copy> --no-banner --redact=100 --report-format json --report-path target/quality/reports/gitleaks-source-doctor-secrets-icu.json --exit-code 1`: passed, 0 entries.
- Direct source unsafe scan over `crates/cdf-cli/src/commands.rs`, `crates/cdf-cli/src/tests.rs`, and `crates/cdf-cli/tests/doctor_env.rs`: passed, empty report.
- `jscpd --silent --reporters json --output target/quality/reports/jscpd-doctor-secrets-icu crates/cdf-cli/src/commands.rs crates/cdf-cli/src/tests.rs crates/cdf-cli/tests/doctor_env.rs`: metric recorded 21 exact clones, 198 duplicated lines, 6.55%, 0 new clones.
- `cargo deny check advisories`: passed earlier in the closure loop.
- `cargo deny check`: exited 4 with `advisories ok, bans ok, licenses FAILED, sources ok`; license policy remains owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.
- `cargo vet`: exited 255 because `supply-chain/` is not initialized; this remains owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.

## What this supports or challenges

This supports closing the doctor secrets and DuckDB ICU child ticket: the scoped behavior is implemented, tested, mutation-checked, and security-scanned. The known full `cargo deny` license-policy and `cargo vet` blockers are not caused by this slice and already have a durable supply-chain owner.

## Limits

This evidence does not implement new secret providers, OS keychain integration, status freshness, inspect-run story assembly, OTLP export, or package archive behavior. DuckDB ICU availability is intentionally environment-dependent; tests assert structured reporting and safe behavior rather than a specific local ICU outcome. `cargo geiger` was not run because `.10x/knowledge/quality-gate-execution.md` records that it can destroy normal target output in this repository; a direct source unsafe scan was used for this scoped CLI change.
