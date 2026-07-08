Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-cli-surface.md, .10x/tickets/done/2026-07-05-dlt-shim-preview.md, .10x/tickets/done/2026-07-06-split-existing-rust-crate-roots.md, .10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md, .10x/tickets/done/2026-07-06-improve-codeql-rust-extractor-coverage.md

# CLI, dlt, and crate-split quality gates

## What was observed

The current commit batch passed the applicable `QUALITY.md` deep loop for CLI plumbing, dlt preview shim, and crate-root split work. The reusable CodeQL database path is `target/quality/codeql-db-rust`; it was rebuilt in place only after source files became newer than the existing database.

The CLI ticket remains blocked on missing lower-layer runtime, SQL, recovery, registry, migration, and retention APIs. The dlt preview and crate-root split tickets satisfy their scoped acceptance criteria.

## Passing command results

- `git diff --check -- . ':(exclude).gitignore'`: passed.
- `python3 -m compileall -q python/cdf_sdk python/examples`: passed.
- `uvx pyright python/cdf_sdk python/examples`: passed with 0 errors.
- `cargo fmt --all -- --check`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo check --workspace --all-targets --all-features --locked`: passed.
- `cargo check --workspace --all-targets --no-default-features --locked`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings`: passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast`: passed, 125 tests.
- `cargo test --workspace --all-targets --all-features --locked --no-fail-fast`: passed, 125 tests.
- `cargo nextest run --workspace --locked`: passed, 125 tests.
- `cargo hack check --workspace --all-targets --each-feature --locked`: passed.
- `cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings`: passed.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed, 0 doctests.
- `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo llvm-cov --workspace --all-features --locked --summary-only`: passed; total coverage was 71.90% regions, 71.62% functions, and 74.97% lines.
- `cargo metadata --format-version=1 --locked`, `cargo tree --workspace --locked`, and `cargo tree --workspace --locked -d`: passed.
- `cargo audit --json`: passed.
- `cargo deny check advisories`: passed.
- `cargo machete`: passed, no unused dependency candidates.
- `osv-scanner scan source -r . --format json`: passed with 0 vulnerabilities.
- `semgrep scan --config p/rust --error`: passed with 0 findings after the CLI argv/test-directory fixes.
- `semgrep scan --config p/security-audit --error`: passed with 0 findings.
- Source-only `gitleaks dir` scan excluding `.git`, `target`, and `reports`: passed with no leaks.
- `gitleaks git`: passed with no leaks across 5 commits.
- `rust-code-analysis-cli -m -p crates -O json`: passed.
- `jscpd . --reporters json,console --ignore '**/target/**,**/.git/**,**/reports/**'`: exited 0; Rust duplicated lines were 310/24232, 1.28%, and total duplicated lines were 1177/41145, 2.86%.
- `cargo semver-checks --workspace --baseline-rev HEAD`: passed for all library packages after restoring `ObservedSchema` derives. `cdf-cli` was excluded because this batch intentionally added a new library target to a baseline binary-only package, which `cargo-semver-checks` cannot compare as a baseline library.
- `cargo bloat --release -p cdf-cli --bin cdf-cli -n 20`: passed. The `.text` section was 28.6 MiB and file size was 45.1 MiB; top symbols were dominated by bundled DuckDB/DataFusion code, with `cdf_cli::commands::dispatch` as the first CDF symbol at 44.3 KiB.
- `codeql database create target/quality/codeql-db-rust --language=rust --source-root . --overwrite --command 'env CARGO_TARGET_DIR=target/codeql-cargo-target cargo check --workspace --all-targets --locked' --codescanning-config target/quality/codeql-rust-config.yml`: passed.
- `codeql database analyze target/quality/codeql-db-rust codeql/rust-queries --format=sarif-latest --output=target/quality/reports/codeql-rust-current-batch.sarif`: passed with 0 non-diagnostic findings.

Final pre-commit focused recheck after record closure and ticket moves:

- `git diff --check -- . ':(exclude).gitignore'`: passed.
- `cargo fmt --all -- --check`: passed.
- `python3 -m compileall -q python/cdf_sdk python/examples`: passed.
- `uvx pyright python/cdf_sdk python/examples`: passed with 0 errors.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast`: passed, 125 tests.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.

## Policy-blocked or limited commands

- `cargo deny check`: failed only at the license policy stage because CDF still has no ratified `deny.toml` license allowlist. Advisories, bans, and sources were ok. Owner: `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.
- `cargo vet`: failed because `supply-chain/` is not initialized. Owner: `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.
- Raw `gitleaks dir` over the entire repository found generated `target/**` artifacts, including prior redacted reports and bundled generated third-party source. Source-only and git-history scans were clean, so no tracked-source secret was found.
- `cargo geiger --all-features` does not run cleanly from the virtual workspace manifest. A package-mode run was noisy and exceeded a reasonable sidecar budget. Direct first-party source search found no `unsafe`, `unsafe impl`, `unsafe trait`, FFI, raw pointer conversions, `transmute`, or `MaybeUninit`; matches were ordinary `Send`/`Sync` bounds and prose.
- CodeQL produced 0 findings but reported limited Rust extractor coverage: 113 Rust files scanned, 80 extracted with errors, and 33 without errors. This is owned by `.10x/tickets/done/2026-07-06-improve-codeql-rust-extractor-coverage.md`.
- Miri, `cargo careful`, sanitizers, `cargo +nightly udeps`, fuzzing, Kani, Criterion, `cargo bench`, and `cargo mutants` were not run for this batch. The local toolchain is stable-only for nightly checks, no configured fuzz/proof/benchmark harness exists for this surface, no first-party unsafe was introduced, and this broad mixed batch is already covered by workspace tests, nextest, feature matrix, coverage, semver, scanners, and CodeQL.

## What this supports or challenges

This supports committing the dlt preview shim, crate-root split work, and the practical blocked CLI surface. It also supports leaving the CLI ticket blocked rather than pretending unsupported runtime-sensitive commands are implemented.

This challenges the repository-level quality story in two durable places only: supply-chain policy still needs ratification, and local CodeQL Rust extractor coverage still needs improvement or a documented acceptance limit.

## Limits

Tool success does not prove absence of defects. The quality evidence does not close CLI runtime acceptance, does not ratify license or cargo-vet policy, and does not prove behavior outside executed tests and scanner coverage.
