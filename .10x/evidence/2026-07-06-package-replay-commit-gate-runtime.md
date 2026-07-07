Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-package-replay-commit-gate-runtime.md

# Prepared package commit-gate runtime verification

## What was observed

`cdf-project` now exposes a focused prepared-package DuckDB/SQLite runtime API in `crates/cdf-project/src/runtime.rs`, exported through a thin crate root. The API has two entry points: `replay_prepared_duckdb_package` for a new destination commit and checkpoint, and `recover_prepared_duckdb_package` for the committed-before-checkpointed crash window when a durable receipt already exists.

The runtime validates the package before mutation, rejects non-replayable packages, compares package hash and schema hash to the caller-supplied `StateDelta`, validates exact segment id/count/byte coverage, constructs `DestinationCommitRequest` from explicit caller values with `idempotency_token = package_hash`, proposes the checkpoint before destination work, verifies a durable DuckDB receipt before `CheckpointStore::commit`, and supports receipt-based recovery without source contact. Package lifecycle status uses `Loading` before destination work and `Checkpointed` only after checkpoint commit.

`crates/cdf-project/src/lib.rs` remains non-monolithic: it declares `runtime` and `runtime_tests` modules and re-exports the public runtime API.

The pre-existing dirty `.gitignore` was not part of this ticket and remained unstaged.

## Procedure and results

- Focused verification passed:
  - `cargo fmt --all -- --check`
  - `git diff --check`
  - `cargo test -p cdf-project --locked --no-fail-fast`: 24 tests passed.
  - `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`
  - `cargo test -p cdf-dest-duckdb -p cdf-state-sqlite --locked --no-fail-fast`: 9 DuckDB tests and 16 SQLite tests passed.
  - `cargo nextest run -p cdf-project --locked`: 24 tests passed.
  - `cargo check --workspace --all-targets --locked`
- Focused mutation passed: `cargo mutants --package cdf-project --file crates/cdf-project/src/runtime.rs --no-shuffle --jobs 4 --timeout 120 --output target/quality/reports/mutants-prepared-package-runtime -- --locked` tested 27 mutants: 23 caught, 4 unviable, 0 missed.
- Workspace compile, feature, lint, test, and docs gates passed:
  - `cargo metadata --format-version=1 --locked`
  - `cargo tree --workspace --locked`
  - `cargo tree --workspace --locked -d`
  - `cargo check --workspace --all-targets --locked`
  - `cargo check --workspace --all-targets --all-features --locked`
  - `cargo check --workspace --all-targets --no-default-features --locked`
  - `cargo clippy --workspace --all-targets --locked -- -D warnings`
  - `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
  - `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings`
  - `cargo test --workspace --all-targets --locked --no-fail-fast`: 220 tests passed.
  - `cargo test --workspace --all-targets --all-features --locked --no-fail-fast`: 220 tests passed.
  - `cargo test --workspace --doc --all-features --locked --no-fail-fast`
  - `cargo nextest run --workspace --locked`: 220 tests passed.
  - `cargo doc --workspace --all-features --no-deps --locked`
  - `cargo hack check --workspace --all-targets --each-feature --locked`: 17 crates checked.
  - `cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings`: 17 crates checked.
- Coverage passed: `cargo llvm-cov --workspace --all-features --locked --summary-only` reported total 77.83% region coverage, 74.76% function coverage, and 80.54% line coverage. New `cdf-project/src/runtime.rs` reported 85.82% region coverage, 90.91% function coverage, and 80.08% line coverage.
- Public API compatibility passed: `cargo semver-checks --workspace --baseline-rev HEAD`.
- Dependency hygiene and maintainability gates passed or recorded:
  - `cargo machete`: no unused dependencies.
  - `cargo +nightly udeps --workspace --all-targets --locked`: all dependencies used.
  - `rust-code-analysis-cli -m -p crates -O json -o target/quality/reports/prepared-package-runtime/rust-code-analysis`: 136 metric files emitted.
  - `jscpd . --reporters json,console --output target/quality/reports/prepared-package-runtime/jscpd --ignore "**/target/**,**/.git/**,**/reports/**,**/.10x/evidence/.storage/**,**/.10x/research/.storage/**"`: duplicate percentage 4.086787447075285, 205 clones, 2249 duplicated lines.
  - `tokei . --output json`: Rust code count 30314.
  - `scc --format json .`: total code count 30391.
- Security and supply-chain gates passed:
  - `cargo audit --json`: 0 vulnerabilities.
  - `cargo deny check`: advisories, bans, licenses, and sources ok.
  - `cargo vet`: succeeded with 385 current-version exemptions.
  - `osv-scanner scan source -r . --format json --output target/quality/reports/prepared-package-runtime/osv.json`: 0 vulnerabilities.
  - `semgrep scan --config p/rust --error --json --output target/quality/reports/prepared-package-runtime/semgrep-rust.json --exclude target --exclude .git --exclude reports .`: 0 findings.
  - `semgrep scan --config p/security-audit --error --json --output target/quality/reports/prepared-package-runtime/semgrep-security.json --exclude target --exclude .git --exclude reports .`: 0 findings.
  - `gitleaks git --no-banner --redact --report-format json --report-path target/quality/reports/prepared-package-runtime/gitleaks-git.json .`: 0 findings.
  - `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/prepared-package-runtime/gitleaks-dir.json /tmp/cdf-gitleaks-runtime-src`: 0 findings over a temporary source mirror including tracked files plus the new runtime files.
  - `tools/codeql-rust-quality.sh`: refreshed the reusable database at `target/quality/codeql-db-rust` because Rust source, manifests, and lockfile changed; SARIF `target/quality/reports/codeql-rust-current.sarif` had 0 results.
- CodeQL limits: local CodeQL CLI 2.25.6 scanned 136 Rust files and reported 0 extraction errors, 1813 extraction warnings, 2017 macro calls, and 1951 unresolved macro calls. This matches the active `.10x/knowledge/quality-gate-execution.md` note about current Rust extractor macro limitations.
- Unsafe and soundness checks:
  - Direct source inventory over `crates/cdf-project`, `crates/cdf-dest-duckdb`, `crates/cdf-state-sqlite`, `crates/cdf-package`, and `crates/cdf-kernel` found only existing `Send`/`Sync` trait bounds and async type aliases; no Rust `unsafe`, FFI declarations, raw pointer conversions, transmute, or `MaybeUninit` in the touched owned surface.
  - `cargo geiger --manifest-path /Users/alexanderbut/code_projects/personal/cdf/crates/cdf-project/Cargo.toml --all-features --include-tests --dev-dependencies --locked --output-format Json` with isolated `CARGO_TARGET_DIR=target/quality/geiger-runtime-target` completed. `cdf-project` has 0 used unsafe functions, expressions, impls, traits, and methods; the scanned first-party dependency set also totals 0 used unsafe items.
  - `cargo +nightly careful test -p cdf-project --all-features --locked`: 24 tests passed.
  - `cargo +nightly miri test -p cdf-project --locked runtime_tests::replay_rejects_non_replayable_package_before_checkpoint_or_destination_mutation` compiled but first failed under default isolation because the test creates temporary directories. With `MIRIFLAGS=-Zmiri-disable-isolation`, Miri reached the test and then stopped on unsupported `rusqlite` native SQLite FFI (`sqlite3_threadsafe`). This is a Miri tool limitation for the current runtime test, not a failing assertion.
- Formal/fuzz tooling:
  - Installed and set up Kani via `cargo install --locked kani-verifier` and `cargo kani setup`; `cargo kani -p cdf-project` completed and reported no `#[kani::proof]` harnesses to verify.
  - `cargo +nightly fuzz list` reported no `fuzz/Cargo.toml`, so no fuzz targets are configured for this repository slice.
- Tooling installation required by `QUALITY.md` and the user request was completed for previously missing user-level tools: `cargo-expand 1.0.123`, `cargo-flamegraph 0.6.13`, `cargo-insta 1.48.0`, `kani/cargo-kani 0.67.0`, `tokei 14.0.0`, and `scc 3.7.0`. These were not added to workspace manifests.

## What this supports or challenges

This supports the ticket acceptance criteria: the runtime rejects invalid/non-replayable packages before mutation, does not infer semantic execution values from package filenames or ids, uses the package hash as the destination idempotency token, proposes state before destination work, abandons the proposed checkpoint on pre-receipt destination failure, verifies durable receipts before checkpoint commit, leaves state unadvanced on receipt verification/identity/ack failures, exposes a narrow post-receipt verification hook, and supports recovery by committing an already proposed checkpoint from a supplied durable DuckDB receipt without source contact.

This also supports the user's crate-organization requirement: the implementation lives in focused module files rather than growing `lib.rs`.

## Limits

This evidence covers only the prepared-package DuckDB/SQLite runtime primitive. It does not implement live source extraction, full `cdf run`, CLI `resume` or `replay package` command wiring, a generic destination abstraction, package GC retention, chaos killpoints, golden fixtures, or the full MVP killer demo.

Miri did not execute the runtime assertion because the test path depends on `rusqlite` FFI, which Miri cannot call on macOS. The runtime surface itself adds no owned unsafe code, and Careful plus Geiger/source-search passed.
