Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-destination-conformance-suite-foundation.md, .10x/specs/destination-receipts-guarantees.md, .10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md

# Destination conformance suite foundation evidence

## What was observed

The destination conformance foundation adds a reusable `cdf-conformance::destination` harness and DuckDB/Parquet consumer tests. Parent-observed quality checks support the child ticket acceptance criteria, with two pre-existing supply-chain policy gaps remaining owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.

## Procedure and results

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed before record closure.
- `cargo test -p cdf-conformance --locked --no-fail-fast`: passed, 12 tests.
- `cargo test -p cdf-dest-duckdb --locked --no-fail-fast`: passed, 9 tests.
- `cargo test -p cdf-dest-parquet --locked --no-fail-fast`: passed, 15 tests.
- `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`: passed after repairing a duplicate-branch lint.
- `cargo clippy -p cdf-dest-duckdb --all-targets --locked -- -D warnings`: passed.
- `cargo clippy -p cdf-dest-parquet --all-targets --locked -- -D warnings`: passed.
- `cargo mutants -p cdf-conformance --file crates/cdf-conformance/src/destination/mod.rs --jobs 4 -o target/quality -- --locked -p cdf-conformance -p cdf-dest-duckdb -p cdf-dest-parquet --no-fail-fast`: passed with 13 mutants tested, 7 caught, 6 unviable, 0 missed. Raw outcome file: `target/quality/mutants.out/outcomes.json`.
- `cargo metadata --format-version=1 --locked --no-deps`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo check --workspace --all-targets --all-features --locked`: passed.
- `cargo check --workspace --all-targets --no-default-features --locked`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings`: passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast`: passed.
- `cargo test --workspace --all-targets --all-features --locked --no-fail-fast`: passed.
- `cargo nextest run --workspace --locked`: passed, 190 tests.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed, no doctests.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo hack check --workspace --all-targets --each-feature --locked`: passed.
- `cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings`: passed.
- `cargo llvm-cov --workspace --all-features --locked --summary-only`: passed. Workspace totals: 76.53% regions, 73.78% functions, 79.31% lines. New `crates/cdf-conformance/src/destination/mod.rs`: 96.72% regions, 92.59% functions, 97.31% lines.
- `cargo audit`: passed, 402 crate dependencies scanned, no vulnerabilities reported.
- `cargo deny check`: failed only on the known unratified license policy; advisories, bans, and sources were ok. Existing owner: `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.
- `cargo deny check advisories`: passed.
- `cargo vet`: failed because `supply-chain/` is not initialized. Existing owner: `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.
- `osv-scanner scan source -r . --format json --output target/quality/osv.json`: passed, 0 results.
- `gitleaks dir` over the full working directory initially reported generated `target/**` and historical report noise only. A source snapshot excluding `.git`, `target`, and reports passed with empty JSON: `target/quality/gitleaks-dir-source.json`.
- `gitleaks git --no-banner --redact --report-format json --report-path target/quality/gitleaks-git.json .`: passed with empty JSON.
- `semgrep scan --config p/rust --error --json --output target/quality/semgrep-rust.json .`: passed, 0 results.
- `cargo machete`: passed, no unused dependencies.
- `cargo semver-checks --workspace --baseline-rev HEAD` with `CARGO_TARGET_DIR=target/quality/semver-target`: passed, no semver update required.
- `rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|Unpin|Send|Sync" crates`: found no unsafe blocks or FFI in CDF source; matches were trait bounds/type aliases or string text.
- `tools/codeql-rust-quality.sh`: passed. The reusable database path was `target/quality/codeql-db-rust`; it regenerated only because Rust source/manifest/lock inputs changed. SARIF `target/quality/reports/codeql-rust-current.sarif` has 0 results. CodeQL reported 0 extraction errors and the known Rust extractor warning class recorded in `.10x/knowledge/quality-gate-execution.md`.
- `cargo tree --workspace --locked` and `cargo tree --workspace --locked -d`: passed and wrote tree snapshots under `/tmp`.
- `rust-code-analysis-cli -m -p crates -O json -o target/quality/rust-code-analysis`: passed and emitted per-file JSON metrics under `target/quality/rust-code-analysis/crates/**`.
- `jscpd . --reporters json,console --output target/quality/jscpd --ignore "**/target/**,**/.git/**,**/reports/**"`: ran successfully. It reported existing duplicate-code gradients: 2073 duplicated lines total, 4.13%; Rust 1084 duplicated lines, 3.35%.
- `cargo +nightly udeps --workspace --all-targets`: passed; all dependencies appeared used.
- `cargo geiger --manifest-path crates/cdf-cli/Cargo.toml --all-features --all-dependencies --include-tests --locked --output-format Ratio`: completed analysis and wrote `target/quality/geiger-ratio.txt`, then exited nonzero because dependency scanner warnings were treated as errors. The summary showed CDF crates at 100% safe ratios; the dependency-wide ratio was 94.07% functions, 94.12% expressions, 96.41% items, 94.95% impls, 96.95% lines. A narrower conformance retry was stopped after it again behaved as a dependency-wide warning-heavy scan without producing a useful report.

## What this supports

The reusable destination harness is exercised by both self-tests and real DuckDB/Parquet consumer tests. It catches false sheet claims, wrong idempotency, wrong target/disposition echoing, wrong delivery guarantees, wrong migrations, missing type mappings, accepting unsupported dispositions, and planning migrations when migration support is unsupported. Mutation testing found no surviving mutants in the new destination conformance module.

Workspace formatting, compile, clippy, test, doctest, docs, feature, coverage, semver, static-analysis, vulnerability, secret-scan, duplicate-code, unused-dependency, and CodeQL checks were run according to `QUALITY.md` as applicable to this slice.

## Limits

`cargo deny check` and `cargo vet` remain blocked by unratified supply-chain policy, not by this destination-conformance implementation. `cargo geiger` was limited by scanner warnings from registry/generated dependency files; direct source search found no CDF unsafe blocks or FFI. Miri, cargo-careful, fuzzing, Kani, benchmarks, and bloat checks were not run because this slice added no unsafe code, proof harnesses, fuzz targets, or performance-sensitive runtime paths.
