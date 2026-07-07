Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-local-file-run-duckdb-checkpoint.md

# Local file run to DuckDB checkpoint evidence

## What was observed

The explicit local-file `firn run` slice now executes one declarative local file resource through `firn-engine`, writes a package under the selected environment package root, commits the package to a local DuckDB destination, verifies the DuckDB receipt, and commits a SQLite checkpoint only after receipt verification.

The implementation is scoped to:

- `crates/firn-engine`: semver-additive segment source-position reporting for package execution.
- `crates/firn-project`: reusable local file to DuckDB/SQLite run orchestration plus receipt/checkpoint recovery tests.
- `crates/firn-cli`: explicit `run --resource --pipeline --target --package-id --checkpoint-id` wiring, JSON/human output, and no-write negative tests.

## Behavioral evidence

- `cargo test -p firn-engine -p firn-project -p firn-cli --locked --no-fail-fast` passed after the final mutation-hardening patch. This covered the focused engine segment-position test, project live-run and failure-window tests, and CLI success/negative tests.
- `cargo nextest run -p firn-engine -p firn-project -p firn-cli --locked` passed with 108 focused tests.
- `cargo test --workspace --all-targets --locked --no-fail-fast` passed with 289 tests, including live Postgres tests and the new local file run cases.
- `cargo nextest run --workspace --locked` passed with 289 tests.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast` passed; all workspace doctest targets had 0 doctests.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked` passed.
- `cargo llvm-cov --workspace --all-features --locked --summary-only` passed under instrumentation. Total summary: 25,750 lines, 5,475 missed, 78.74% line coverage; 1,936 functions, 482 missed, 75.10% function coverage; 18,825 regions, 3,481 missed, 81.51% region coverage.

Acceptance-specific test coverage includes:

- Successful CLI run writes package, DuckDB rows, `_firn_loads` / `_firn_state` mirror evidence, and SQLite checkpoint head.
- Human output mentions receipt verification and crossing the firn line.
- JSON output includes explicit command/resource/pipeline/target/package/checkpoint/receipt/write-effect fields.
- Missing explicit inputs, non-DuckDB destinations, REST resources, SQL resources, existing package dirs, path-like package ids, discovered-schema resources, and `--loop` fail before package/destination/checkpoint writes.
- Project-level tests reject non-file resources, mismatched plan package ids, divergent segment source positions, bad receipts, missing segment acknowledgements, and premature checkpoint commits.
- The injected post-receipt failure test leaves the package and durable verified receipt recoverable while leaving SQLite head unadvanced.

## Quality evidence

Formatting, compile, lint, feature, and docs:

- `cargo fmt --all -- --check` passed.
- `cargo check --workspace --all-targets --locked` passed.
- `cargo check --workspace --all-targets --all-features --locked` passed.
- `cargo check --workspace --all-targets --no-default-features --locked` passed.
- `cargo hack check --workspace --all-targets --each-feature --locked` passed across 17 packages.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` passed.
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings` passed.
- `cargo metadata --locked --format-version 1` passed and wrote `target/quality/cargo-metadata-local-file-run.json`.
- `cargo tree --workspace --locked` and `cargo tree --workspace --locked -d` completed. Duplicate dependency output remains the known Arrow/DataFusion/DuckDB graph shape and is also covered by `cargo deny check`.

Dependency, supply-chain, and security:

- `cargo deny check` passed: advisories, bans, licenses, and sources OK. It still prints known duplicate-version warnings.
- `cargo audit` passed after scanning 429 crate dependencies.
- `cargo vet --locked` passed with `Vetting Succeeded (412 exempted)`.
- `osv-scanner --lockfile Cargo.lock` passed with no issues found across 429 packages.
- `cargo machete` passed with no unused dependencies.
- `cargo +nightly udeps --workspace --all-targets --locked` passed with all deps used.
- `semgrep scan --config p/rust --error --json --output target/quality/semgrep-local-file-run-rust-final.json crates/firn-engine crates/firn-project crates/firn-cli` passed with 0 findings.
- `semgrep scan --config p/security-audit --error --json --output target/quality/semgrep-local-file-run-security-final.json crates/firn-engine crates/firn-project crates/firn-cli` passed with 0 findings.
- `tools/codeql-rust-quality.sh` completed using the reusable database path `target/quality/codeql-db-rust`. It refreshed the DB only because Rust source/manifest/lockfile content changed, wrote `target/quality/reports/codeql-rust-current.sarif`, and produced 0 SARIF findings. Wrapper summary: 147/147 Rust files scanned, 0 extraction errors, 2,279 extraction warnings, 2,593 unresolved macro calls. The warning profile matches the known local CodeQL Rust macro-expansion limit recorded in `.10x/knowledge/quality-gate-execution.md`.
- Direct first-party source scan over touched crates for `unsafe`, unsafe impl/trait, FFI, raw pointer conversions, `transmute`, `MaybeUninit`, and explicit `Send`/`Sync` surfaces returned no matches.
- `gitleaks git --no-banner --redact --report-format json --report-path target/quality/gitleaks-local-file-run-git.json .` passed before record closure.
- Source-bearing Gitleaks scans for `crates`, `.10x`, `Cargo.lock`, `tools`, and `supply-chain` passed before record closure. A full working-tree `gitleaks dir` scan was not used as the hard signal because it reported generated `target/**` false positives from tool outputs and vendored build artifacts.
- After closure records were written, `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/gitleaks-local-file-run-10x-final.json .10x` passed with no leaks found.

API compatibility, maintainability, and metrics:

- `CARGO_TARGET_DIR=target/quality/semver-target cargo semver-checks --workspace --baseline-rev HEAD` passed for all workspace crates with no semver update required.
- `jscpd --min-lines 8 --min-tokens 80 --reporters console crates/firn-project/src/runtime.rs crates/firn-cli/src/commands.rs` passed with 0 clones.
- `rust-code-analysis-cli -m -O json -p crates/firn-project/src/runtime.rs` completed. Highest reported function cyclomatic values in the touched runtime file were 14 for `replay_prepared_duckdb_package`, 12 for `run_local_file_to_duckdb_checkpoint`, and 10 for `state_delta_from_run`; these are orchestration functions with focused negative tests and mutation coverage.
- `tokei crates/firn-engine/src crates/firn-project/src crates/firn-cli/src --output json` completed. It reported 11,033 Rust code lines across those three source trees.
- `scc --format json crates/firn-engine/src crates/firn-project/src crates/firn-cli/src` completed. It reported 8,999 code lines across 25 Rust files.
- `git diff --check -- . ':(exclude).gitignore'` passed before record closure.

Mutation testing:

- Initial bounded runtime mutation run:
  - Command: `cargo mutants --package firn-project --file crates/firn-project/src/runtime.rs --cargo-arg=--locked --jobs 2 --test-tool cargo --timeout 900 --output target/quality/mutants-local-file-run-runtime -- -p firn-project -p firn-cli -- --nocapture`
  - Result: 45 mutants tested; 33 caught, 9 unviable, 3 missed.
  - Misses mapped to real acceptance gaps: local run resource validation returning `Ok(())`, run-plan validation returning `Ok(())`, and divergent segment source-position comparison changed from `!=` to `==`.
- Hardened tests were added for non-file resource rejection before writes, plan/package id mismatch before writes, and divergent segment source-position rejection.
- Final bounded runtime mutation rerun:
  - Command: same as above with output `target/quality/mutants-local-file-run-runtime-rerun`.
  - Result: 45 mutants tested; 36 caught, 9 unviable, 0 missed. `missed.txt` is empty.

## Limits

- This evidence proves only the explicit local file resource to local DuckDB destination and SQLite checkpoint slice. REST/SQL resources, non-DuckDB destinations, multi-resource runs, automatic run ids, run ledger, `resume`, and `replay package` CLI wiring remain explicitly outside this ticket.
- CodeQL still has the known local Rust extractor macro-warning limit. The hard evidence is the successful process exit, 0 SARIF findings, and 0 `ExtractionErrors` query results.
- `cargo geiger` was not rerun as a hard gate for this ticket because `.10x/knowledge/quality-gate-execution.md` records local Geiger target-cache side effects, the touched first-party crates have no unsafe/FFI/raw-pointer matches, and this slice does not introduce unsafe code.
- Miri, cargo-careful, sanitizers, fuzzing, Kani, benchmarks, binary-size, and profilers were not run because this ticket does not add unsafe code, custom concurrency primitives, configured fuzz/proof harnesses, or performance-sensitive binary-size targets.
