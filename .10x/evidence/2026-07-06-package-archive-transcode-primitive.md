Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-package-archive-transcode-primitive.md

# Package archive transcode primitive verification

## What was observed

`cdf-package` now exposes `archive_package_to_parquet` and a serializable in-memory archive report. The primitive opens and verifies a package before reading IPC segments, transcodes each segment to Parquet bytes through the existing DuckDB-backed writer path, and records package hash, fidelity statement, source IPC path/hash/bytes/rows, Parquet bytes/hash/bytes/rows, and segment id.

The DuckDB Parquet writer implementation moved from `cdf-dest-parquet` into focused `cdf-package` module files. `cdf-dest-parquet` delegates to the shared writer, so destination commit, object-store, manifest, and receipt semantics remain in the destination crate. `crates/cdf-package/src/lib.rs` remains a thin module/export root rather than a monolithic implementation file.

Parent review found one missed mutation: removing duplicate-column validation from the shared Parquet writer survived the first mutation run. The implementation now has a package-level duplicate-column prevalidation test, and the final mutation run has 0 missed mutants.

The pre-existing dirty `.gitignore` was not part of this ticket and remained unstaged.

## Procedure and results

- Re-read the active child ticket, the Singer/Airbyte/package-archive parent, and the crate-organization knowledge record. Result: scope stayed limited to the package archive transcode primitive and fidelity report; CLI command wiring, archive file placement, manifest archive metadata, retention, and package lifecycle mutation remain excluded.
- Focused worker verification passed before parent review: `cargo fmt --all -- --check`; `cargo test -p cdf-package --locked --no-fail-fast`; `cargo test -p cdf-dest-parquet --locked --no-fail-fast`; `cargo clippy -p cdf-package -p cdf-dest-parquet --all-targets --locked -- -D warnings`; `cargo deny check advisories`; `git diff --check -- . ':(exclude).gitignore'`; and `rg -n '^name = "(parquet|paste)"' Cargo.lock crates/cdf-package/Cargo.toml crates/cdf-dest-parquet/Cargo.toml` with no matches.
- Parent review ran `cargo mutants --package cdf-package --file crates/cdf-package/src/archive.rs --file crates/cdf-package/src/parquet.rs --locked --output target/quality/mutants-archive-primitive`. First run: 27 mutants, 1 missed, 20 caught, 6 unviable. The missed mutant was `replace validate_field_names -> Result<()> with Ok(())`.
- After adding duplicate-column prevalidation coverage, final `cargo mutants --package cdf-package --file crates/cdf-package/src/archive.rs --file crates/cdf-package/src/parquet.rs --locked --output target/quality/mutants-archive-primitive` passed: 27 mutants, 0 missed, 21 caught, 6 unviable.
- Final compile, formatting, test, documentation, and API gates passed:
  - `cargo metadata --format-version=1 --locked --no-deps`
  - `cargo fmt --all -- --check`
  - `cargo nextest run --workspace --locked --no-fail-fast`: 196 tests passed.
  - `cargo clippy --workspace --all-targets --locked -- -D warnings`
  - `cargo hack check --workspace --all-targets --locked`: all 17 workspace crates checked.
  - `cargo +nightly udeps --workspace --all-targets --locked`: all dependencies used.
  - `cargo test --doc --workspace --locked`: all workspace doctests passed.
  - `cargo doc --workspace --no-deps --locked`: documentation generated.
  - `cargo semver-checks check-release --workspace --baseline-rev HEAD`: passed before the final test-string-only patch; no semver update required.
- Final coverage gate passed: `cargo llvm-cov --workspace --locked --summary-only`. Total coverage was 76.64% regions and 79.34% lines. New archive files measured at `cdf-package/src/archive.rs` 80.39% line coverage and `cdf-package/src/parquet.rs` 58.01% line coverage.
- Final dependency and supply-chain gates passed:
  - `cargo deny check`: advisories, bans, licenses, and sources ok.
  - `cargo audit`: scanned 402 locked crate dependencies, no vulnerabilities.
  - `cargo vet --locked --output-format json --output-file target/quality/cargo-vet-archive-primitive-final.json`: `conclusion: success`, 0 fully audited crates, 0 partially audited crates, 385 current-version exemptions.
  - `osv-scanner scan source -r . --format json --output target/quality/osv-archive-primitive-final.json`: 0 results.
  - `cargo machete`: no unused dependencies.
  - `rg -n '^name = "(parquet|paste)"|parquet =|paste =' Cargo.lock crates/cdf-package/Cargo.toml crates/cdf-dest-parquet/Cargo.toml`: no matches.
- Final security/static gates passed:
  - `semgrep scan --config auto --error --json --output target/quality/semgrep-archive-primitive-final.json crates/cdf-package crates/cdf-dest-parquet`: 0 findings.
  - `tools/codeql-rust-quality.sh`: reused fresh database `target/quality/codeql-db-rust`; SARIF `target/quality/reports/codeql-rust-current.sarif` has 0 results. Extractor diagnostics remained noisy but non-blocking: 132 Rust files scanned, 0 extraction errors, 1636 extraction warnings.
  - `gitleaks detect --no-git` over a temporary mirror of tracked plus untracked non-ignored source files: no leaks, empty report `target/quality/gitleaks-archive-source-tracked-final.json`.
  - A broad `gitleaks detect --no-git --source .` intentionally was not used as the source gate because it scanned generated `target/` trees and prior JSON reports, producing 360 generated-artifact hits. The scoped source mirror is the commit-surface secret scan.
  - `cargo geiger` over absolute manifests for `cdf-package` and `cdf-dest-parquet` completed with exit 0 and JSON reports in `target/quality/`; the touched first-party crates have 0 unsafe functions, expressions, impls, traits, and methods. Dependency graphs contain expected third-party unsafe counts and produced two third-party parse warnings.
  - Direct source search `rg -n '\bunsafe\b|extern "|\*const|\*mut|unsafe impl|impl (Send|Sync)' crates/cdf-package crates/cdf-dest-parquet`: no matches.
- Final mechanical checks passed:
  - `git diff --check -- . ':(exclude).gitignore'`
  - `git status --short --ignored=matching` showed only intended tracked/untracked source and 10x changes plus unrelated `.gitignore` and ignored build/cache artifacts.

## What this supports or challenges

This supports the ticket acceptance criteria: package verification happens before transcode work; tampered packages fail before a report is returned; one Parquet byte vector is produced per IPC segment; the report includes source and Parquet byte/hash/row metadata; the fidelity statement preserves Arrow IPC as canonical; the primitive does not write archive files or mutate manifest/lifecycle/identity/hash state; repeat runs are deterministic; IPC replay/read APIs still read IPC segments; and the existing Parquet destination behavior is preserved through delegation.

This also supports the supply-chain constraint that this slice does not add the direct arrow-rs `parquet` crate or the blocked `paste` advisory path.

## Limits

This evidence covers only the in-memory archive transcode primitive and shared writer extraction. It does not prove CLI command behavior, archive file placement, manifest archive metadata, retention/GC behavior, package lifecycle status transitions, or tombstone/archive deletion workflows. Those remain owned by `.10x/tickets/done/2026-07-05-singer-airbyte-and-package-archive.md`.
