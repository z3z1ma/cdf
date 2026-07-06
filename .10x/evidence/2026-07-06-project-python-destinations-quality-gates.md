Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md, .10x/tickets/done/2026-07-05-python-sdk-bridge.md, .10x/tickets/done/2026-07-05-duckdb-destination.md, .10x/tickets/2026-07-05-postgres-destination.md, .10x/tickets/done/2026-07-06-replace-include-crate-splits-with-modules.md, .10x/tickets/2026-07-06-ratify-supply-chain-policy.md, .10x/tickets/2026-07-06-split-existing-rust-crate-roots.md, .10x/tickets/2026-07-06-improve-codeql-rust-extractor-coverage.md

# Project, Python, and destination quality gates

## What was observed

The integrated project/Python/DuckDB/Postgres batch passed the primary compile, test, lint, doc, scanner, and metric gates after the final module refactor. Quality reports were written under `target/quality/reports/`, and the reusable CodeQL database was refreshed at `target/quality/codeql-db-rust` only after source/dependency changes.

## Procedure

Ran the `QUALITY.md` loop at production depth for this batch: formatting, feature-aware checks, full tests, nextest, clippy, docs, coverage, dependency graph reports, dependency hygiene, public API compatibility, Python typing, duplication/complexity metrics, security scanners, secret scans, and CodeQL.

## Passing command results

- `cargo fmt --all` and `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo check --workspace --all-targets --all-features --locked`: passed before and after the module refactor.
- `cargo check --workspace --all-targets --no-default-features --locked`: passed before and after the module refactor.
- `cargo test -p firn-project -p firn-dest-postgres -p firn-dest-duckdb -p firn-python --locked --no-fail-fast`: passed with 46 unit tests and 0 doctests.
- `cargo test --workspace --all-targets --locked --no-fail-fast`: passed after the final module refactor with 114 unit tests and 0 failures.
- `cargo test --workspace --all-targets --all-features --locked --no-fail-fast`: passed after the final module refactor with 114 unit tests and 0 failures.
- `cargo clippy -p firn-project -p firn-dest-postgres -p firn-dest-duckdb -p firn-python --all-targets --locked -- -D warnings`: passed before and after the module refactor.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed after the final module refactor.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: passed before the final module refactor.
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings`: passed before the final module refactor.
- `cargo hack check --workspace --all-targets --each-feature --locked`: passed all 17 workspace packages.
- `cargo nextest run --workspace --locked`: passed 114 tests.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo llvm-cov --workspace --all-features --locked --summary-only`: passed with 85.03% line coverage and 83.72% region coverage.
- `cargo metadata --format-version=1 --locked`, `cargo tree --workspace --locked`, and `cargo tree --workspace --locked -d`: passed; reports saved under `target/quality/reports/`.
- `cargo machete`: passed with no unused dependency candidates.
- `cargo semver-checks --baseline-rev HEAD~1`: passed all workspace packages, no semver update required.
- `python3 -m compileall -q python/firn_sdk python/examples && uvx pyright python/firn_sdk python/examples`: passed with 0 errors.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-project-python-destinations.json .`: passed with 0 findings.
- `cargo audit --json > target/quality/reports/cargo-audit-project-python-destinations-final.json`: passed with 0 vulnerabilities.
- `cargo deny check advisories`: passed.
- `osv-scanner scan source -r . --format json --output target/quality/reports/osv-project-python-destinations-final.json`: passed with 0 results.
- Source-only `gitleaks dir` scan excluding `.git`, `target`, and `reports`: passed with 0 findings.
- `gitleaks git . --redact=100 --report-format json --report-path target/quality/reports/gitleaks-git-project-python-destinations.json`: passed with 0 findings.
- `jscpd . --reporters json,console --output target/quality/reports/jscpd-project-python-destinations --ignore "**/target/**,**/.git/**,**/reports/**"`: exited 0. Rust duplicated tokens were 1.96%; total report includes expected record/tooling text noise.
- `rust-code-analysis-cli -m -p crates -O json -o target/quality/reports/rust-code-analysis-project-python-destinations`: passed.
- Direct owned-source unsafe search with `rg -n "unsafe\\s*(fn|impl|trait|\\{|extern)|transmute|MaybeUninit|from_raw|into_raw|raw pointer|\\*const|\\*mut" crates --glob "*.rs"`: no matches.
- Final CodeQL database creation with `target/quality/codeql-rust-config.yml` excluding `target/**` and `reports/**`: passed and refreshed `target/quality/codeql-db-rust`.
- Final `codeql database analyze target/quality/codeql-db-rust codeql/rust-queries --format=sarif-latest --output=target/quality/reports/codeql-rust-project-python-destinations-final.sarif`: passed with 0 SARIF findings.

## Policy-blocked or limited commands

- `cargo deny check`: failed at license policy because no repository `deny.toml` or license allowlist is ratified. Advisories, bans, and sources passed; licenses failed under the default deny behavior. Owner: `.10x/tickets/2026-07-06-ratify-supply-chain-policy.md`.
- `cargo vet`: failed because `supply-chain/` metadata does not exist. Owner: `.10x/tickets/2026-07-06-ratify-supply-chain-policy.md`.
- Raw `gitleaks dir .` over the whole repository reported 64 findings in generated `target/` build output from bundled DuckDB/mbedTLS/Parquet sources. Source-only and git-history scans were clean; no tracked source secret was found.
- `cargo geiger`: attempted through package and manifest-path modes. This installed version does not accept the virtual workspace root and produced repeated registry package-match diagnostics for the current dependency graph when run per crate; the run was stopped after it exceeded a reasonable sidecar budget. Direct owned-source unsafe search was clean.
- Miri, cargo-careful, sanitizers, fuzzing, Kani, cargo-udeps, mutation testing, benchmarks, cargo-bloat, and profilers were not run for this batch because they require nightly/project harnesses/performance workloads or are disproportionate to the changed surfaces. No owned unsafe code was introduced.

## CodeQL limits

The final CodeQL run produced 0 findings and no `include` macro expansion failures for the active-batch crate roots after `.10x/tickets/done/2026-07-06-replace-include-crate-splits-with-modules.md`. The local Rust extractor still reported generic macro expansion diagnostics for standard and third-party macros and summarized 41 extracted-with-error files and 9 without-error files. This limits CodeQL confidence; it does not contradict the other compile/test/lint/scanner evidence. The remaining extractor friction is tracked by `.10x/tickets/2026-07-06-improve-codeql-rust-extractor-coverage.md`.

## What this supports or challenges

This supports accepting the project/Python/DuckDB implemented surfaces, the Postgres planning surface with its live-execution blocker, and the crate-organization refactor. It also confirms no source secret, RustSec advisory, OSV vulnerability, Semgrep finding, clippy warning, formatting drift, or workspace test failure was present at the end of the batch.

## Limits

This evidence does not close `.10x/tickets/2026-07-05-postgres-destination.md`, because live Postgres execution evidence is still missing. It does not ratify license or cargo-vet policy, and it does not prove absence of bugs outside executed tests and scanner coverage.
