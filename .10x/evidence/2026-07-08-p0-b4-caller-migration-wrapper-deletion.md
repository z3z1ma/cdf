Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-07-p0-b4-caller-migration-wrapper-deletion.md, .10x/tickets/done/2026-07-07-p0-workstream-b-open-orchestrator-world.md

# P0 B4 Caller Migration and Wrapper Deletion Evidence

## What Was Observed

B4 migrated CLI replay/resume and conformance package-replay/live-run callers to generic package replay/recovery APIs and deleted the public destination-specialized wrapper family from `cdf-project`.

Changed source shape:

```text
20 files changed, 597 insertions(+), 1364 deletions(-)
```

Deleted or de-exported public compatibility names include:

- `PackageArtifactDuckDbReplayRequest`, `PackageArtifactDuckDbRecoveryRequest`
- `PackageArtifactParquetReplayRequest`, `PackageArtifactParquetRecoveryRequest`
- `PackageArtifactPostgresReplayRequest`, `PackageArtifactPostgresRecoveryRequest`
- `PreparedDuckDbReplayRequest`, `PreparedDuckDbRecoveryRequest`, `PreparedDuckDbReplayReport`
- `PreparedParquetReplayReport`, `PreparedPostgresReplayReport`
- `PreparedReceiptSource`, `LocalDuckDbLifecycleFailpoint`
- `replay_duckdb_package_from_artifacts`, `recover_duckdb_package_from_artifacts`
- `replay_parquet_package_from_artifacts`, `recover_parquet_package_from_artifacts`
- `replay_postgres_package_from_artifacts`, `recover_postgres_package_from_artifacts`
- `replay_prepared_duckdb_package`, `recover_prepared_duckdb_package`
- DuckDB-only failpoint wrapper variants.

The old wrapper-family name scan over project, CLI, and conformance Rust source returned no matches:

```text
rg -n "PackageArtifactDuckDb|PackageArtifactParquet|PackageArtifactPostgres|PreparedDuckDbReplayRequest|PreparedDuckDbRecoveryRequest|PreparedDuckDbReplayReport|PreparedParquetReplayReport|PreparedPostgresReplayReport|PreparedReceiptSource|replay_duckdb_package_from_artifacts|recover_duckdb_package_from_artifacts|replay_parquet_package_from_artifacts|recover_parquet_package_from_artifacts|replay_postgres_package_from_artifacts|recover_postgres_package_from_artifacts|replay_prepared_duckdb_package|recover_prepared_duckdb_package|LocalDuckDbLifecycleFailpoint" crates/cdf-project/src crates/cdf-cli/src crates/cdf-conformance/src -g '*.rs'
exit 1, no matches
```

Remaining public replay/recovery surface is generic:

- `PackageArtifactReplayRequest`
- `PackageArtifactRecoveryRequest`
- `PreparedPackageReplayRequest`
- `PreparedPackageRecoveryRequest`
- `PackageReplayReport`
- `ProjectReceiptSource`
- `replay_package_from_artifacts`
- `recover_package_from_artifacts`
- `replay_prepared_package`
- `recover_prepared_package`
- `replay_package_from_artifacts_with_stage_hook`
- `replay_prepared_package_with_stage_hook`

`crates/cdf-cli/src/replay_command.rs` and `crates/cdf-cli/src/resume_command/*` now resolve destinations through the project runtime and call the generic artifact replay/recovery functions. `crates/cdf-conformance/src/package_replay/*` and live-run tests now use the generic request/report types. `cdf-project` replay validates the resolved destination target against package commit target before checkpoint proposal.

The runtime module split after B4:

```text
   75 crates/cdf-project/src/runtime.rs
  474 crates/cdf-project/src/runtime/artifacts.rs
  495 crates/cdf-project/src/runtime/destinations.rs
   14 crates/cdf-project/src/runtime/hooks.rs
  154 crates/cdf-project/src/runtime/ledger.rs
  233 crates/cdf-project/src/runtime/orchestration.rs
   94 crates/cdf-project/src/runtime/receipts.rs
  571 crates/cdf-project/src/runtime/replay.rs
   78 crates/cdf-project/src/runtime/resources.rs
  129 crates/cdf-project/src/runtime/types.rs
  148 crates/cdf-project/src/runtime/validation.rs
   90 crates/cdf-project/src/runtime/destinations/duckdb.rs
  135 crates/cdf-project/src/runtime/destinations/parquet.rs
  270 crates/cdf-project/src/runtime/destinations/postgres.rs
 2960 total
```

Direct unsafe/soundness source scan over the touched project runtime, CLI, conformance package-replay, and conformance live-run source found no `unsafe`, extern blocks, raw pointers, transmute, or manual `Send`/`Sync` impls.

## Procedure

Focused Rust checks:

```text
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo check -p cdf-project -p cdf-cli -p cdf-conformance --all-targets
cargo clippy -p cdf-project -p cdf-cli -p cdf-conformance --all-targets -- -D warnings
cargo hack check -p cdf-project -p cdf-cli -p cdf-conformance --all-targets --each-feature --locked
cargo hack clippy -p cdf-project -p cdf-cli -p cdf-conformance --all-targets --each-feature --locked -- -D warnings
cargo test -p cdf-project runtime_tests -- --nocapture
cargo test -p cdf-cli replay_ -- --nocapture
cargo test -p cdf-cli resume_ -- --nocapture
cargo test -p cdf-conformance package_replay -- --nocapture
cargo test -p cdf-conformance live_run -- --nocapture
cargo nextest run -p cdf-project -p cdf-cli -p cdf-conformance --locked
cargo test --doc -p cdf-project -p cdf-cli -p cdf-conformance --locked
cargo doc -p cdf-project -p cdf-cli -p cdf-conformance --no-deps --locked
git diff --check
```

Quality/security checks:

```text
semgrep scan --config p/rust --error --json --output target/quality/reports/b4/semgrep-rust.json --no-git-ignore crates/cdf-project/src crates/cdf-cli/src crates/cdf-conformance/src
gitleaks dir crates/cdf-project/src --no-banner --redact --report-format json --report-path target/quality/reports/b4/gitleaks-project-src.json
gitleaks dir crates/cdf-cli/src --no-banner --redact --report-format json --report-path target/quality/reports/b4/gitleaks-cli-src.json
gitleaks dir crates/cdf-conformance/src --no-banner --redact --report-format json --report-path target/quality/reports/b4/gitleaks-conformance-src.json
osv-scanner scan source -r . --format json --output target/quality/reports/b4/osv.json
cargo deny check > target/quality/reports/b4/cargo-deny-check.txt
cargo audit --json > target/quality/reports/b4/cargo-audit.json
cargo vet --locked > target/quality/reports/b4/cargo-vet-locked.txt
tools/codeql-rust-quality.sh > target/quality/reports/b4/codeql-rust-quality.stdout
jscpd crates/cdf-project/src/runtime.rs crates/cdf-project/src/runtime crates/cdf-project/src/runtime_tests.rs crates/cdf-cli/src crates/cdf-conformance/src --reporters json --output target/quality/reports/b4/jscpd
rust-code-analysis-cli -m -p crates/cdf-project/src/runtime -O json -o target/quality/reports/b4/rust-code-analysis-runtime
scc --format json crates/cdf-project/src/runtime.rs crates/cdf-project/src/runtime crates/cdf-project/src/runtime_tests.rs crates/cdf-cli/src crates/cdf-conformance/src > target/quality/reports/b4/scc.json
cargo semver-checks -p cdf-project --baseline-rev HEAD > target/quality/reports/b4/semver-cdf-project.txt
```

## Results

Focused Rust verification passed:

- `cargo fmt --all --check`
- `cargo check --workspace --all-targets --locked`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `cargo check -p cdf-project -p cdf-cli -p cdf-conformance --all-targets`
- `cargo clippy -p cdf-project -p cdf-cli -p cdf-conformance --all-targets -- -D warnings`
- `cargo hack check -p cdf-project -p cdf-cli -p cdf-conformance --all-targets --each-feature --locked`
- `cargo hack clippy -p cdf-project -p cdf-cli -p cdf-conformance --all-targets --each-feature --locked -- -D warnings`
- `cargo test -p cdf-project runtime_tests -- --nocapture`: 49 passed
- `cargo test -p cdf-cli replay_ -- --nocapture`: 14 passed
- `cargo test -p cdf-cli resume_ -- --nocapture`: 9 passed
- `cargo test -p cdf-conformance package_replay -- --nocapture`: 11 passed
- `cargo test -p cdf-conformance live_run -- --nocapture`: 4 passed
- `cargo nextest run -p cdf-project -p cdf-cli -p cdf-conformance --locked`: 208 passed
- `cargo test --doc -p cdf-project -p cdf-cli -p cdf-conformance --locked`
- `cargo doc -p cdf-project -p cdf-cli -p cdf-conformance --no-deps --locked`
- `git diff --check`

Security and supply-chain:

- Semgrep: 0 findings, 0 errors.
- Gitleaks: 0 leaks across project, CLI, and conformance source reports.
- OSV: one vulnerability, `paste 1.0.15 / RUSTSEC-2024-0436`, already ratified as the scoped paste advisory.
- `cargo audit --json`: 0 vulnerabilities; one unmaintained warning for `paste 1.0.15 / RUSTSEC-2024-0436`, already ratified.
- `cargo deny check`: exit 0; final summary `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo vet --locked`: exit 0; `Vetting Succeeded (393 exempted)`.
- CodeQL: reused `target/quality/codeql-db-rust`; SARIF `target/quality/reports/codeql-rust-current.sarif` has one run, `executionSuccessful=true`, and 0 results. The wrapper reported 189 Rust files scanned and the known local Rust extractor macro-warning noise, not security findings.

Quality metrics:

- `jscpd`: 58 sources, 24,695 lines, 161 clones, 1,684 duplicated lines (6.819%), 12,109 duplicated tokens (8.088%).
- `scc`: 59 Rust files, 24,703 lines, 18,871 code lines, 4,496 comment lines, 1,336 blank lines, aggregate complexity 886.
- `rust-code-analysis-cli` module cyclomatic totals: `replay.rs` 114, `destinations.rs` 107, `artifacts.rs` 97, `destinations/postgres.rs` 64, `validation.rs` 41, `orchestration.rs` 37, `destinations/parquet.rs` 31, `ledger.rs` 28, `destinations/duckdb.rs` 25.
- `rust-code-analysis-cli` top function/impl totals: `replay_package_with_runtime` 19, `close_cursor_value` 19, Postgres driver `resolve` 17, `run_project_inner` 14.

`cargo semver-checks -p cdf-project --baseline-rev HEAD` exited 1 with 5 major check categories. All failures are intentional B4 public API removals or generic stage enum changes under the pre-1.0 P0 refactor:

- Removed public enums: `PreparedReceiptSource`, `LocalDuckDbLifecycleFailpoint`.
- Added generic `RuntimeStage` variants: `PackageReplayVerified`, `DestinationWriteReady`; existing variant discriminants changed because the enum is exhaustive and had no explicit representation.
- Removed public destination-specific functions, including DuckDB/Parquet/Postgres artifact replay/recovery wrappers, prepared DuckDB replay/recovery wrappers, and DuckDB-only failpoint wrappers.
- Removed public destination-specific request/report structs and the test-only local DuckDB run request/report surface from the normal public API.

## What This Supports

B4 acceptance is satisfied:

- CLI replay package and resume callers route through generic project-owned destination resolution and generic package artifact replay/recovery APIs.
- Conformance package-replay, live-run, golden, and chaos helper callers route through the generic path.
- The destination-specialized public wrapper names are absent from project, CLI, and conformance Rust source.
- The B2/B3 registered mock destination proof remains in place for adding a destination by registration rather than generic orchestrator edits.
- `runtime.rs` is no longer monolithic; runtime concerns are split across focused modules.
- The remaining semver failures are deliberate removal of the temporary specialized public API surface that this ticket existed to delete.

## Limits

This evidence closes B4 and Workstream B, not the P0 stop-line. Workstream C still owns the conformance matrix, non-DuckDB chaos breadth, per-destination live-run goldens, and property/fuzz targets.
