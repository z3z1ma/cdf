Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-python-doctor-interpreter-probe.md, .10x/tickets/2026-07-05-observability-doctor-status-sql.md

# Python doctor interpreter probe

## What was observed

`firn doctor` now performs a process-based Python interpreter probe in `crates/firn-cli/src/commands.rs`. The probe resolves the configured interpreter relative to the project root, requires an existing executable file, runs only a fixed inline inspection snippet as `-I -c <snippet>`, parses structured JSON, validates version/GIL metadata consistency, requires Python 3.12 or newer, and enforces `python.require_free_threaded = true` only when the probe reports a free-threaded build with the GIL disabled.

Focused CLI tests in `crates/firn-cli/src/tests.rs` cover skip/no-resource behavior, Python resources without an interpreter, fixed-snippet invocation that excludes project Python resource code, GIL-enabled pass when free-threading is not required, free-threaded pass, required-free-threaded failures, missing and non-executable interpreter failures, unsuccessful probes and invalid JSON without secret echo, inconsistent probe metadata failures, old-version failure, and the existing read-only DuckDB drift no-create regression.

## Procedure

The following checks passed after the final helper refactor unless a limit is named below:

- `cargo fmt --all -- --check`
- `git diff --check -- . ':(exclude).gitignore'`
- `cargo test -p firn-cli --locked --no-fail-fast`
- `cargo test --workspace --all-targets --locked --no-fail-fast`
- `cargo test --doc --workspace --locked`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `cargo nextest run --workspace --locked`: 180 tests run, 180 passed.
- `cargo doc --workspace --no-deps --locked`
- `cargo hack check --workspace --feature-powerset --locked`: 17 packages checked.
- `CARGO_TARGET_DIR=target/quality/semver-target cargo semver-checks --workspace --baseline-rev HEAD`: no semver update required.
- `cargo machete`: no unused dependencies found.
- `cargo deny check advisories`: `advisories ok`.
- `cargo audit --json > target/quality/reports/cargo-audit-python-doctor-probe.json`: `.vulnerabilities.found = false`, `.vulnerabilities.count = 0`.
- `osv-scanner scan --format json --output-file target/quality/reports/osv-python-doctor-probe.json .`: `.results | length = 0`.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-python-doctor-probe.json --exclude target --exclude reports --exclude .git .`: `.results | length = 0`.
- `gitleaks dir <source-copy> --no-banner --redact=100 --report-format json --report-path target/quality/reports/gitleaks-source-python-doctor-probe.json --exit-code 1`: report length 0.
- Direct owned-source unsafe search over the changed CLI files wrote an empty `target/quality/reports/unsafe-rg-python-doctor-probe.txt`.
- `tools/codeql-rust-quality.sh`: refreshed the reusable database in place at `target/quality/codeql-db-rust` because the stored input fingerprint was missing/stale for the current Rust inputs, wrote `target/quality/codeql-db-rust/firn-codeql-inputs.sha256`, analyzed successfully, and produced `target/quality/reports/codeql-rust-current.sarif` with 0 SARIF results. The run scanned 128 Rust files and printed `Extraction errors: 0`; it also reported the known local Rust extractor macro warning limit.
- `jscpd --silent --reporters json --output target/quality/reports/jscpd-python-doctor-probe crates/firn-cli/src/commands.rs crates/firn-cli/src/tests.rs`: exited 0 and reported 5.19% exact duplication in the scoped files. This is a metric, not a failure; the repeated CLI tests intentionally exercise distinct doctor outcomes.

Mutation testing was run with:

```text
git diff -- crates/firn-cli/src/commands.rs > /tmp/firn-python-doctor-commands.diff
cargo mutants -p firn-cli --file crates/firn-cli/src/commands.rs --in-diff /tmp/firn-python-doctor-commands.diff --test-package firn-cli --jobs 4 --timeout 120 --output target/quality/reports/mutants-python-doctor-probe -- --locked
```

The first run tested 36 mutants, caught 29, missed 5, and found 2 unviable. The actionable misses led to extra tests for missing-interpreter details, probe/setup failure details, inconsistent version/GIL metadata, and free-threaded-build-with-GIL-enabled rejection.

The rerun wrote `target/quality/reports/mutants-python-doctor-probe-rerun/mutants.out/outcomes.json` and reported 36 total mutants, 33 caught, 1 missed, 2 unviable, and 0 timeouts. The remaining missed mutant is `crates/firn-cli/src/commands.rs:904:5: replace is_executable -> bool with false`, which mutates the `#[cfg(not(unix))]` fallback branch that is not compiled on this macOS run. The Unix executable check path is covered by `doctor_fails_non_executable_python_interpreter`.

## Limits

Full `cargo deny check` and `cargo vet` still fail for existing repository policy reasons: no ratified license allowlist/`deny.toml` and no initialized `supply-chain/` metadata. This remains owned by `.10x/tickets/2026-07-06-ratify-supply-chain-policy.md`; advisory-only deny, cargo-audit, and OSV passed for this slice.

`cargo geiger` was attempted during this closure using a package manifest and normal target output. The tool removed a large amount of Cargo build cache before the run was interrupted. This did not change tracked source or the reusable CodeQL database, but it is not closure authority for this ticket. `.10x/knowledge/quality-gate-execution.md` now records the stronger local rule: avoid non-isolated Geiger runs, and pair any future isolated Geiger run with direct first-party unsafe source search.

The tests use fake interpreter scripts to deterministically exercise Python version and free-threaded/GIL metadata because the local environment does not provide every required CPython build variant. The fixed invocation test asserts the configured interpreter receives exactly `-I -c <fixed inspection snippet>` and that project Python resource identifiers/code markers are absent from the snippet.
