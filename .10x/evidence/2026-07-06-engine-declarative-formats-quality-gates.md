Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-datafusion-engine-planner.md, .10x/tickets/done/2026-07-05-declarative-resources.md, .10x/tickets/done/2026-07-05-formats-and-subprocess.md, .10x/tickets/2026-07-06-parquet-format-source-supply-chain.md, .10x/tickets/2026-07-06-ratify-supply-chain-policy.md

# Engine, declarative, formats, and subprocess quality gates

## What was observed

The engine, declarative resource compiler, formats adapters, and subprocess adapter batch passed the applicable workspace quality gates after the Parquet source reader was split to `.10x/tickets/2026-07-06-parquet-format-source-supply-chain.md`.

The reusable CodeQL database was created at `target/quality/codeql-db-rust` for this source/dependency state and analyzed in place. Future quality passes should reuse this directory unless Rust source, dependency resolution, or CodeQL tooling has changed enough to require rebuilding the database.

## Procedure

- Ran formatting, build, feature matrix, lint, unit, nextest, doc, coverage, dependency, security, static-analysis, duplicate-code, and metadata checks from `QUALITY.md`.
- Parallelized independent checks where safe after Cargo compilation artifacts were warm.
- Reused generated reports under `target/quality/reports`.
- Kept `.gitignore` unstaged because it contains an unrelated pre-existing change.

## Command results

Passed:

- `cargo fmt --all -- --check`
- `cargo check --workspace --all-targets --locked`
- `cargo check --workspace --all-targets --all-features --locked`
- `cargo check --workspace --all-targets --no-default-features --locked`
- `cargo hack check --workspace --all-targets --each-feature --locked`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings`
- `cargo test --workspace --all-targets --locked --no-fail-fast`: 73 unit tests passed.
- `cargo test --workspace --all-targets --all-features --locked --no-fail-fast`: 73 unit tests passed.
- `cargo nextest run --workspace --locked`: 68 tests passed.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast`: 0 doctests, passed.
- `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked`
- `cargo llvm-cov --workspace --all-features --locked --summary-only`: total line coverage 87.65%, total region coverage 86.15%.
- `cargo metadata --format-version=1 --locked`
- `cargo tree --workspace --locked`
- `cargo tree --workspace --locked -d`
- `cargo machete`
- `rust-code-analysis-cli -m -p crates -O json -o target/quality/reports/rust-code-analysis`
- `jscpd . --reporters json,console --output target/quality/reports/jscpd --ignore '**/target/**,**/.git/**,**/reports/**'`: Rust duplicated tokens 2.16%.
- `cargo audit --json > target/quality/reports/cargo-audit.json`
- `cargo deny check advisories`
- `osv-scanner scan source -r . --format json --output-file target/quality/reports/osv.json`
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust.json .`: 0 findings.
- `gitleaks dir --source . --report-format json --report-path target/quality/reports/gitleaks-dir.json`
- `gitleaks git --source . --report-format json --report-path target/quality/reports/gitleaks-git.json`
- `cargo semver-checks --baseline-rev HEAD~1`
- `codeql database create target/quality/codeql-db-rust --language=rust --source-root . --command 'cargo check --workspace --all-targets --locked'`
- `codeql database analyze target/quality/codeql-db-rust codeql/rust-queries --format=sarif-latest --output=target/quality/reports/codeql-rust.sarif`: 0 SARIF results.
- `git diff --check`

Blocked or limited:

- `cargo deny check` still fails at the license policy stage because there is no ratified Firn license allowlist or `deny.toml`; this is owned by `.10x/tickets/2026-07-06-ratify-supply-chain-policy.md`.
- `cargo vet` still fails because `supply-chain/` is not initialized; this is owned by `.10x/tickets/2026-07-06-ratify-supply-chain-policy.md`.
- `cargo geiger` does not run successfully from the virtual workspace manifest. A package-manifest run for `firn-kernel` reported no owned unsafe usage before dependency warning noise; direct source search found no Firn-owned unsafe blocks, unsafe impls, unsafe traits, FFI, raw pointer functions, `transmute`, or `MaybeUninit`.
- CodeQL reported extractor diagnostics: 17 of 17 Rust files scanned for the invocation, with 23 extracted-with-error diagnostic entries and 8 extracted-without-error diagnostic entries. The SARIF security result count was 0.
- Missing tools from `QUALITY.md` were not installed because the corresponding gates were not applicable to this batch: `tokei`, `scc`, `cargo-expand`, `cargo-flamegraph`, `cargo-insta`, and nightly `miri`.

## What this supports or challenges

This supports closing the engine, declarative, and formats/subprocess tickets with the current Parquet source exclusion. It also supports the new Parquet supply-chain blocker and the existing supply-chain policy ticket as the remaining quality-governed blockers.

## Limits

The quality pass does not ratify license policy, cargo-vet policy, or the Parquet source dependency choice. Those remain separate Outer Loop decisions/tickets and must not be treated as implemented by passing advisory scanners after the Parquet split.
