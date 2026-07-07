Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/2026-07-07-general-run-orchestrator.md, .10x/tickets/2026-07-07-general-run-postgres-destination.md, .10x/tickets/2026-07-07-general-run-non-file-resource-streams.md

# General Run Orchestrator Partial Verification

## What Was Observed

The `cdf-project` runtime now has a `ProjectRunRequest` / `run_project` facade that runs deterministic declarative local file resources into DuckDB and filesystem Parquet destinations through package-aware destination planning and the kernel `DestinationProtocol::begin` session path.

The legacy `run_local_file_to_duckdb_checkpoint` compatibility wrapper delegates into `run_project` while preserving the legacy `LocalFileDuckDbRunReport` and `PreparedReceiptSource` shapes. Project-level reports use destination-neutral `ProjectReceiptSource`.

The run ledger records ordered events around package finalization, checkpoint proposal, destination commit start, receipt recording, checkpoint commit, package status update, success, and failure. Checkpoint advancement remains through `CheckpointStore::commit`.

## Procedure

- Inspected the diff in `crates/cdf-project/src/runtime.rs`, `crates/cdf-project/src/runtime_tests.rs`, `crates/cdf-project/Cargo.toml`, `Cargo.lock`, and the related tickets.
- Confirmed `Cargo.lock` only gained the direct workspace dependency edge from `cdf-project` to `cdf-dest-parquet`.
- Ran `cargo fmt --all -- --check`: passed.
- Ran `git diff --check`: passed.
- Ran `cargo test -p cdf-project --locked --no-fail-fast`: 42 unit tests passed, 0 doc tests.
- Ran `cargo nextest run -p cdf-project --locked`: 42 tests passed.
- Ran `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`: passed.
- Ran `cargo test -p cdf-dest-parquet --locked --no-fail-fast`: 18 unit tests passed, 0 doc tests.
- Ran `cargo test -p cdf-conformance --locked --no-fail-fast`: 40 unit tests passed, 0 doc tests, including 100-run golden proofs.
- Ran `cargo test -p cdf-cli --locked run_local_file_to_duckdb_commits_package_rows_mirrors_and_checkpoint`: 1 matched test passed.
- Ran `cargo check --workspace --all-targets --locked`: passed.
- Ran `cargo semver-checks -p cdf-project --baseline-rev HEAD`: 196 checks passed, 57 skipped, no semver update required.
- Ran `cargo hack check -p cdf-project --all-targets --each-feature --locked`: passed.
- Ran `semgrep scan --config p/rust --error crates/cdf-project/src`: 0 findings across 9 tracked files.
- Ran direct touched-source unsafe scan with `rg` over `crates/cdf-project/src` and `crates/cdf-cli/src`: no matches.
- Ran `cargo deny check`: passed with existing duplicate-version warnings and no policy failures.
- Ran `cargo audit`: passed with only the ratified allowed `RUSTSEC-2024-0436` warning for `paste`.
- Ran `cargo vet --locked`: passed.
- Ran `cargo machete --with-metadata`: no unused dependencies found.
- Ran `osv-scanner --lockfile Cargo.lock`: exited nonzero only for ratified `RUSTSEC-2024-0436` on `paste` 1.0.15 with no fixed version.
- Ran targeted `gitleaks detect --no-git --redact` over `crates/cdf-project`, `crates/cdf-cli`, and the updated/new ticket records: no leaks found.
- Ran targeted `gitleaks detect --no-git --redact` over this evidence record and `.10x/reviews/2026-07-07-general-run-orchestrator-partial-review.md`: no leaks found.
- Ran `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- Ran `cargo test --workspace --locked --no-fail-fast`: passed; workspace unit/doc/integration tests completed, including live Postgres tests.
- Ran `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --locked`: passed.
- Ran `tools/codeql-rust-quality.sh`: refreshed the reusable `target/quality/codeql-db-rust` database because its input fingerprint was missing, then analyzed with `--rerun`; SARIF result count was 0. Extractor metrics: 153 Rust files total, 35 without extractor errors, 118 with extractor-side extraction issues, 0 extraction errors, 2734 extraction warnings, 3253 unresolved macro calls. These warnings match the local CodeQL extractor limitation recorded in `.10x/knowledge/quality-gate-execution.md`.
- Ran `cargo nextest run --workspace --locked`: 379 tests passed, 0 skipped.

## What This Supports

- The implemented slice satisfies deterministic local-file project runs into DuckDB and filesystem Parquet through the commit-session API.
- The run ledger event order is covered for successful DuckDB and Parquet runs and for a failure after durable receipt.
- Unsupported REST/SQL resources and unsupported Parquet merge fail before package, destination, or checkpoint mutation.
- Recovery after durable DuckDB and Parquet package receipts commits the checkpoint and updates package status without reopening the source resource.
- The change preserves existing DuckDB compatibility wrappers and passes semver checks for `cdf-project`.

## Limits

- This does not prove the full `.10x/tickets/2026-07-07-general-run-orchestrator.md` acceptance criteria because Postgres destinations and non-file REST/SQL source streams remain blocked.
- CodeQL was refreshed once because the reusable database had no fingerprint; future runs should reuse the `target/quality/codeql-db-rust` database unless inputs change.
- `cargo geiger`, Miri, cargo-careful, fuzzing, Kani, cargo-mutants, llvm-cov, and benchmarks were not run for this slice. Geiger was substituted with a direct first-party unsafe scan per `.10x/knowledge/quality-gate-execution.md`; the other tools are not proportionate closure gates for this deterministic orchestration refactor.
