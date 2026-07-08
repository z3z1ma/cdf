Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-cdf-cli-unused-parquet-dependency.md, .10x/tickets/done/2026-07-08-cdf-benchmarks-unused-arrow-csv-dependency.md

# Unused dependency cleanup

## What was observed

Full-workspace `cargo machete --with-metadata` initially reported two unused direct dependencies:

```text
cdf-cli -- ./crates/cdf-cli/Cargo.toml:
    cdf-dest-parquet
cdf-benchmarks -- ./crates/cdf-benchmarks/Cargo.toml:
    arrow-csv
```

Targeted source search found no `cdf_dest_parquet` imports in `crates/cdf-cli` and no `arrow_csv` imports in `crates/cdf-benchmarks`. The `arrow-csv` crate remains legitimately used by `cdf-formats`; this cleanup only removed the unused direct benchmark-crate edge.

## Procedure

- Removed `cdf-dest-parquet` from `crates/cdf-cli/Cargo.toml`.
- Removed `arrow-csv` from `crates/cdf-benchmarks/Cargo.toml`.
- Refreshed `Cargo.lock` through normal Cargo check flows.
- Verified direct dependency metadata after the edit:
  - `cdf-cli` direct dependencies no longer include `cdf-dest-parquet`.
  - `cdf-benchmarks` direct dependencies no longer include `arrow-csv`.
- Ran `cargo check -p cdf-cli --all-targets`: passed.
- Ran `cargo check -p cdf-benchmarks --all-targets`: passed.
- Ran `cargo check -p cdf-cli -p cdf-benchmarks --all-targets --locked`: passed.
- Ran `cargo test -p cdf-cli -p cdf-benchmarks --locked --no-fail-fast`: passed; `cdf-cli` reported 103 library tests, 1 integration test, and 0 doc tests; `cdf-benchmarks` reported 3 tests and 0 doc tests.
- Ran `cargo clippy -p cdf-cli -p cdf-benchmarks --all-targets --locked -- -D warnings`: passed.
- Ran `cargo check --workspace --all-targets --locked`: passed.
- Ran `cargo fmt --all -- --check`: passed.
- Ran `git diff --check`: passed.
- Ran `cargo machete --with-metadata > target/quality/reports/cargo-machete-unused-dependency-cleanup.txt`: passed with `cargo-machete didn't find any unused dependencies in this directory. Good job!`
- Ran `cargo deny check > target/quality/reports/cargo-deny-unused-dependency-cleanup.txt 2>&1`: passed; still warns about the previously ratified duplicate Arrow 58.3.0/59.1.0 tuple.
- Ran `cargo audit --json > target/quality/reports/cargo-audit-unused-dependency-cleanup.json`: passed; 0 vulnerabilities and 1 unmaintained warning for the already-ratified `paste` advisory.
- Ran `cargo vet --locked > target/quality/reports/cargo-vet-unused-dependency-cleanup.txt 2>&1`: passed, `Vetting Succeeded (424 exempted)`.
- Ran `osv-scanner scan source -r . > target/quality/reports/osv-unused-dependency-cleanup.json 2>&1`: exited 1 only for the already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` advisory.
- Ran `gitleaks detect --no-git --source crates --report-format json --report-path target/quality/reports/gitleaks-unused-dependency-cleanup.json --no-banner --redact`: passed with no leaks.
- Ran `tools/codeql-rust-quality.sh > target/quality/reports/codeql-unused-dependency-cleanup.log 2>&1`: passed, reusing the stable `target/quality/codeql-db-rust` path and refreshing because manifest/lock inputs changed. `target/quality/reports/codeql-rust-current.sarif` contains 0 results.
- Ran direct first-party unsafe scan over `crates/cdf-cli` and `crates/cdf-benchmarks`: no matches.
- Ran `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-unused-dependency-cleanup.json crates/cdf-cli crates/cdf-benchmarks`: passed with 0 findings across 42 targets.
- Ran Jscpd over `cdf-cli` and `cdf-benchmarks` source/test/bench paths: completed with 96 existing clones, 918 duplicated lines, 6.73%, and 0 new clones.
- Ran `rust-code-analysis-cli` over `cdf-cli` and `cdf-benchmarks`: completed; current hotspots are existing CLI modules, with max cyclomatic 27.0 in `crates/cdf-cli/src/state_command.rs` and max cognitive 24.0 in `crates/cdf-cli/src/inspect_run_command.rs`.
- Ran `scc` over the same focused paths: completed.
- Ran `cargo geiger` for both package manifests with isolated target directories: passed. First-party package summaries show 0 used unsafe items for both `cdf-cli` and `cdf-benchmarks`; Geiger still reports third-party dependency unsafe as expected for the Rust dependency graph.

## What this supports

- `cdf-cli` did not require `cdf-dest-parquet` as a direct dependency for checked, tested, clippy-verified, or workspace-checked paths.
- `cdf-benchmarks` did not require `arrow-csv` as a direct dependency for checked, tested, clippy-verified, or workspace-checked paths.
- Full-workspace `cargo machete --with-metadata` is clean after the cleanup.
- The cleanup did not introduce source-level security findings, first-party unsafe usage, format drift, or compile/test regressions in the checked paths.

## Limits

This evidence does not claim a CLI architecture refactor, benchmark workload change, performance improvement, mutation score, or removal of the ratified supply-chain residuals. OSV still reports the ratified `paste` advisory, and `cargo deny` still reports the ratified duplicate Arrow major warning.
