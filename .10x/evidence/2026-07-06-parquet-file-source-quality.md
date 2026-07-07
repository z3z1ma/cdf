Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md, .10x/tickets/2026-07-05-implement-cdf-system.md

# Parquet file source quality evidence

## What was observed

`cdf-formats` now reads `FileFormat::Parquet` through DuckDB's bundled Parquet reader and bridges DuckDB Arrow 58 batches into CDF's Arrow 59 reader path via Arrow IPC bytes. The implementation did not add the direct arrow-rs `parquet` crate and did not reintroduce `paste`.

The focused reader tests cover Parquet descriptor and batch production, file-manifest source positions, schema hash population, malformed Parquet data errors, package write/replay compatibility, and the single-object JSON reader path found by mutation testing.

## Procedure

- `cargo metadata --format-version=1 --locked --no-deps`: passed.
- `cargo fmt --all -- --check`: passed after final test addition.
- `cargo test -p cdf-formats --locked --no-fail-fast`: passed after final test addition; 6 unit tests and 0 doc tests.
- `cargo clippy -p cdf-formats --all-targets --locked -- -D warnings`: passed after final test addition.
- `cargo check --workspace --all-targets --locked`: passed before the final test-only mutation assertion.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed before the final test-only mutation assertion.
- `cargo nextest run --workspace --locked --no-fail-fast`: passed before the final test-only mutation assertion; 196 tests.
- `cargo hack check --workspace --all-targets --locked`: passed before the final test-only mutation assertion.
- `cargo machete`: passed, no unused dependencies.
- `cargo +nightly udeps --workspace --all-targets --locked`: passed, all dependencies used.
- `cargo llvm-cov --workspace --locked --summary-only`: passed before the final test-only mutation assertion; total line coverage 79.35%, `cdf-formats/src/readers.rs` line coverage 82.92%.
- `CARGO_TARGET_DIR=target/doc-quality cargo test --doc --workspace --locked && CARGO_TARGET_DIR=target/doc-quality cargo doc --workspace --no-deps --locked`: passed.
- `CARGO_TARGET_DIR=target/semver-checks cargo semver-checks check-release --workspace --baseline-rev HEAD`: passed; each crate reported no semver update required.
- `cargo mutants --package cdf-formats --file crates/cdf-formats/src/readers.rs --jobs 4 -C --locked --timeout 300 --output target/quality/mutants-parquet-source-rerun`: passed after adding the single-object JSON assertion; 35 mutants tested, 15 caught, 20 unviable, 0 missed, 0 timeout.
- `cargo audit`: passed, scanning 402 locked dependencies.
- `cargo deny check`: passed; advisories, bans, licenses, and sources ok, with duplicate-version warnings.
- `cargo vet --locked --output-format json --output-file target/quality/cargo-vet-parquet-source-final.json`: passed; `conclusion` was `success`.
- `osv-scanner scan source -r . --format json --output target/quality/osv-parquet-source-final.json`: passed; 0 result entries.
- `semgrep scan --config auto --error --json --output target/quality/semgrep-parquet-source-final.json crates/cdf-formats`: passed; 0 findings.
- Source-only gitleaks mirror of `Cargo.toml`, `Cargo.lock`, `crates/cdf-formats`, and the active ticket into `/tmp`: passed; no leaks in `target/quality/gitleaks-parquet-source-final.json`.
- `tools/codeql-rust-quality.sh`: passed. The reusable database path remained `target/quality/codeql-db-rust`; the wrapper refreshed it only because Rust source/manifests/lockfile content changed. SARIF `target/quality/reports/codeql-rust-current.sarif` had 0 results. Extractor metrics were 132 files scanned, 0 extraction errors, 1657 extraction warnings, 96 files with warnings, 36 without warnings.
- `rg -n '^name = "(parquet|paste)"|parquet =|paste =' Cargo.lock crates/cdf-formats/Cargo.toml crates/cdf-package/Cargo.toml crates/cdf-dest-parquet/Cargo.toml`: no matches.
- `rg -n '\bunsafe\b|extern "|\*const|\*mut|unsafe impl|impl (Send|Sync)' crates/cdf-formats`: no matches.
- `cargo geiger --manifest-path /Users/alexanderbut/code_projects/personal/cdf/crates/cdf-formats/Cargo.toml --all-targets --locked > target/quality/geiger-parquet-source.txt`: produced a usable `cdf-formats 0.1.0` root row of `0/0` functions, expressions, impls, traits, and methods, but exited nonzero because dependency parsing emitted 397 warnings. This matches the known local Geiger limitation in `.10x/knowledge/quality-gate-execution.md`; the direct source unsafe scan above is the hard project-owned unsafe check.
- `git diff --check -- . ':(exclude).gitignore'`: passed.

## What this supports or challenges

This supports closing `.10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md`: Parquet file sources now produce CDF descriptors and batches, malformed inputs map to data errors, package replay compatibility is covered, and the locked dependency graph remains advisory-clean without direct `parquet` or `paste` entries.

The evidence also supports the CodeQL reuse requirement: the reusable database remains under `target/quality/codeql-db-rust`, and it was refreshed only after the content fingerprint changed.

## Limits

Parquet/DuckDB is a lossy format boundary for Arrow-only metadata. Tests assert supported schema names, data types, nulls, values, source positions, and package replay; canonical Arrow IPC remains the exact metadata-preserving package format.

Some workspace-wide gates ran before the final test-only mutation assertion. The final post-assertion gates were focused on `cdf-formats`, mutation testing, security scanners, dependency scanners, CodeQL, and whitespace.
