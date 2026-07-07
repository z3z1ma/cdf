Status: recorded
Created: 2026-07-06
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-06-prepared-package-chaos-conformance.md, .10x/tickets/2026-07-05-conformance-chaos-golden.md, .10x/specs/conformance-governance-roadmap.md, .10x/specs/package-lifecycle-determinism.md, .10x/specs/checkpoint-state-commit-gate.md, .10x/specs/destination-receipts-guarantees.md

# Prepared-package chaos conformance evidence

## What was observed

The prepared-package DuckDB/SQLite chaos conformance slice is implemented in `crates/cdf-conformance/src/package_replay/` and exported from a thin `crates/cdf-conformance/src/lib.rs` module root. The harness builds deterministic Arrow IPC package fixtures with caller-supplied `StateDelta`, target, disposition, schema hash, and merge keys; drives the public `cdf-project` prepared DuckDB replay and recovery APIs; and asserts durable package receipts, DuckDB `_cdf_loads` and `_cdf_state` mirror rows, checkpoint heads, duplicate/no-op replay identity, and recovery from a durable receipt without a second destination write.

The helper-process crash test respawns the current libtest binary only in test code and exits at `PreparedDuckDbReplayRequest::after_receipt_verified`, proving the committed-before-checkpointed boundary across a process boundary. Semgrep initially flagged `env::current_exe()` in this helper; it was left in place with a narrow `nosemgrep` annotation and a test-only rationale because the helper must execute the exact current test binary.

Generated quality artifacts are ignored and stored under `reports/ai-quality/` and reusable `target/quality/` paths. The CodeQL Rust database path reused the project convention at `target/quality/codeql-db-rust`; it refreshed for this slice because Rust sources and dependency metadata changed.

## Procedure and results

Focused ticket gates:

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo test -p cdf-conformance --locked --no-fail-fast`: passed, 23 tests.
- `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`: passed.
- `cargo test -p cdf-project --locked --no-fail-fast`: passed, 24 tests.
- `cargo test -p cdf-dest-duckdb -p cdf-state-sqlite -p cdf-package --locked --no-fail-fast`: passed.

Workspace and `QUALITY.md` checks:

- Tooling was installed and available: stable and nightly Rust toolchains; `cargo-nextest`, `cargo-llvm-cov`, `cargo-hack`, `cargo-deny`, `cargo-audit`, `cargo-vet`, `cargo-machete`, `cargo-udeps`, `cargo-semver-checks`, `cargo-geiger`, `cargo-bloat`, `cargo-mutants`, `cargo-fuzz`, `cargo-careful`, `cargo-kani`, `rust-code-analysis-cli`, `semgrep`, `codeql`, `osv-scanner`, `gitleaks`, `jscpd`, `tokei`, and `scc`.
- `cargo metadata --format-version=1 --locked > reports/ai-quality/cargo-metadata.json`: passed.
- `cargo tree --workspace --locked > reports/ai-quality/cargo-tree.txt`: passed.
- `cargo tree --workspace --locked -d > reports/ai-quality/cargo-tree-duplicates.txt`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo check --workspace --all-targets --all-features --locked`: passed.
- `cargo check --workspace --all-targets --no-default-features --locked`: passed.
- `cargo hack check --workspace --all-targets --each-feature --locked`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings`: passed.
- `cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings`: passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast`: passed.
- `cargo test --workspace --all-targets --all-features --locked --no-fail-fast`: passed.
- `cargo nextest run --workspace --locked`: passed, 226 tests.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed, 0 doctests.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo llvm-cov --workspace --all-features --locked --summary-only`: passed. Total coverage was 78.22% regions and 80.94% lines. `crates/cdf-conformance/src/package_replay/mod.rs` reported 96.44% regions and 98.32% lines. JSON and LCOV reports were written to `reports/ai-quality/llvm-cov.json` and `reports/ai-quality/lcov.info`.
- `cargo audit`: passed, 402 dependencies scanned.
- `cargo deny check`: passed. It still warned about duplicate Arrow 58/59 crate entries, but advisories, bans, licenses, and sources passed.
- `cargo vet`: passed with "Vetting Succeeded (385 exempted)".
- `osv-scanner scan source -r . --format json --output reports/ai-quality/osv.json`: passed with 0 result entries.
- `semgrep scan --config p/rust --error --json --output reports/ai-quality/semgrep-rust.json --exclude target --exclude reports .`: passed after the narrow helper-process suppression; result count was 0.
- `gitleaks git --no-banner --redact --report-format json --report-path reports/ai-quality/gitleaks-git.json .`: passed with 0 findings.
- A first `gitleaks dir` over the repository root was interrupted because it began scanning generated build output. A scoped working-tree copy created from `git ls-files --cached --others --exclude-standard` was then scanned with `gitleaks dir --no-banner --redact --report-format json --report-path reports/ai-quality/gitleaks-dir.json <temp-copy>` and passed with 0 findings.
- `tools/codeql-rust-quality.sh`: passed using reusable database path `target/quality/codeql-db-rust`. `target/quality/reports/codeql-rust-current.sarif` contained 0 results. CodeQL Rust extraction metrics reported 0 extraction errors, 1885 extraction warnings, 138 files extracted, 102 files extracted with errors, 36 files extracted without errors, and 2023 unresolved macro calls, matching the known Rust extractor limitation recorded in `.10x/knowledge/quality-gate-execution.md`.
- `cargo machete`: passed with no unused dependencies.
- `rust-code-analysis-cli -m -p crates -O json -o reports/ai-quality/rust-code-analysis`: passed after creating the output directory.
- `jscpd . --reporters json,console --output reports/ai-quality/jscpd --ignore "**/target/**,**/.git/**,**/reports/**,**/mutants.out/**,**/mutants.out.old/**"`: passed as a metrics gate. Total duplicated lines were 4.07%; Rust duplicated lines were 3.53%. The new relevant similarity is test/harness fixture setup rather than product behavior.
- `tokei . --output json > reports/ai-quality/tokei.json`: passed.
- `scc --format json . > reports/ai-quality/scc.json`: passed.
- `cargo +nightly udeps -p cdf-conformance --all-targets --locked` with isolated target `target/quality/udeps-prepared-package-target`: passed with "All deps seem to have been used."
- `cargo semver-checks --workspace --baseline-rev HEAD`: passed for all crates with no semver update required.
- Direct owned-source unsafe scan with `rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|NonNull|UnsafeCell|raw pointer" crates tools python`: found only `crates/cdf-http/src/retry.rs` string text and no owned unsafe Rust, FFI, or raw-pointer surface in this change.
- `CARGO_TARGET_DIR=target/quality/careful-prepared-package-target cargo +nightly careful test -p cdf-conformance --all-features --locked`: passed, 23 tests.

Mutation testing:

- `cargo mutants --package cdf-conformance --file crates/cdf-conformance/src/package_replay/mod.rs --test-package cdf-conformance --output target/quality/mutants-prepared-package --no-shuffle --jobs 4 --timeout 120 -- --locked`: passed.
- Results under `target/quality/mutants-prepared-package/mutants.out`: 36 mutants tested in about 4 minutes; 20 caught, 16 unviable, 0 missed, and 0 timed out.

Final focused smoke after Semgrep suppression and mutation hardening:

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`: passed.
- `cargo test -p cdf-conformance --locked --no-fail-fast`: passed, 23 tests.
- `cargo test -p cdf-project --locked --no-fail-fast`: passed, 24 tests.

## What this supports

The evidence supports closing the prepared-package chaos conformance foundation ticket: the conformance crate now provides a reusable prepared-package DuckDB/SQLite replay harness; the packaged/no-receipts boundary, duplicate replay identity, committed-before-checkpointed recovery window, failed recovery inputs, and final receipt/checkpoint/mirror comparisons are covered by tests; and the new conformance module is mutation hardened with no missed mutants.

## Limits

- `cargo +nightly fuzz list` could not run project fuzz targets because `fuzz/Cargo.toml` is absent. This slice did not add fuzz infrastructure because the ticket excludes general fuzz/property infrastructure.
- `cargo kani -p cdf-conformance` is structurally blocked by Kani 0.67.0 compiling `rusqlite 0.40.1`, which uses `cfg_select!` requiring an unstable library feature for that verifier toolchain. This does not indicate a failing conformance test.
- `cargo geiger --all-features` from `crates/cdf-conformance` failed after build due third-party dependency parsing and unscanned-file warnings, including `signal-hook-registry-1.4.8/src/lib.rs`. A follow-up `cargo geiger --forbid-only --all-features --locked` attempt repeated the same dependency parser warning for more than a minute and was interrupted with exit code 130. The owned-source unsafe scan above is the recorded project-owned unsafe signal for this slice.
- CodeQL Rust extraction warnings and unresolved macros are retained as a known extractor limitation; the SARIF analysis itself returned 0 results.
