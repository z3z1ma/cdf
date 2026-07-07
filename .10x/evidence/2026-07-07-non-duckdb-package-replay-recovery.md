Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-non-duckdb-package-replay-recovery.md, .10x/tickets/done/2026-07-07-general-run-orchestrator.md, .10x/specs/run-orchestration-ledger.md

# Non-DuckDB package replay recovery evidence

## What was observed

`cdf-project` now exposes package-artifact replay requests for filesystem Parquet and Postgres destinations. Parquet artifact replay derives its replay inputs from package artifacts and drives the existing Parquet package replay path. Postgres artifact replay requires explicit `PostgresTarget`, `MergeDedupPolicy`, and optional `PostgresExistingTable`, validates that the explicit target matches the package destination-commit target, derives columns from the package schema through `cdf-dest-postgres`, derives merge keys from package commit artifacts, plans the load, and drives the existing Postgres replay path.

Durable-receipt recovery functions for Parquet and Postgres remain available and still verify supplied receipts before checkpoint commit.

The new regression tests remove the source file before artifact replay and remove package receipts/reset the destination to exercise no-durable-receipt replay from package artifacts. The tests prove the replay path does not need source contact after package finalization. They simulate the window using package artifacts from a controlled post-receipt failure because Parquet/Postgres do not yet expose named lifecycle failpoints at the exact post-finalization/pre-destination-write boundary.

## Procedure

- `cargo fmt --all -- --check`: passed.
- `git diff --check -- . ':(exclude).gitignore'`: passed.
- Direct `rg` scan for unsafe/FFI/raw-pointer/transmute/`MaybeUninit` in touched project files: no matches.
- `cargo test -p cdf-project --locked artifact_replay -- --nocapture`: passed, 6 tests.
- `cargo test -p cdf-project --locked --no-fail-fast`: passed, 56 unit tests and 0 doc tests.
- `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`: passed.
- `cargo test -p cdf-dest-parquet --locked --no-fail-fast`: passed, 18 unit tests and 0 doc tests.
- `cargo test -p cdf-dest-postgres --locked --no-fail-fast`: passed, 27 unit/live tests and 0 doc tests.
- `cargo test -p cdf-conformance --locked --no-fail-fast`: passed, 40 unit tests and 0 doc tests.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo deny check`: passed with known duplicate-version warnings and no advisory/license/source/bans failures.
- `cargo vet --locked`: passed, `Vetting Succeeded (420 exempted)`.
- `cargo audit --json`: no vulnerabilities; only ratified unmaintained warning `RUSTSEC-2024-0436` for `paste 1.0.15`.
- `osv-scanner --lockfile Cargo.lock --format json --output target/quality/reports/osv-non-duckdb-replay.json`: exited nonzero only for ratified `RUSTSEC-2024-0436`.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-non-duckdb-replay.json crates/cdf-project/src`: passed, 0 findings.
- `codeql database analyze target/quality/codeql-db-rust codeql/rust-queries --format=sarif-latest --output=target/quality/reports/codeql-non-duckdb-replay-existing-db.sarif --rerun`: passed, 0 SARIF results. Limit: this intentionally reused the existing CodeQL DB and reported the known Rust extractor quality limitation.
- `gitleaks git --staged --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-non-duckdb-replay-staged.json .`: passed, no leaks found.

## What this supports

This supports closing `.10x/tickets/done/2026-07-07-non-duckdb-package-replay-recovery.md`:

- Parquet package artifacts can be replayed without a supplied receipt or source file and finish with a verifiable receipt, committed checkpoint, and checkpointed package status.
- Postgres package artifacts can be replayed without a supplied receipt or source file when explicit Postgres destination policy is supplied.
- Postgres target mismatch fails closed before package receipt, destination table, or checkpoint mutation.
- Existing durable-receipt recovery behavior remains covered by unchanged Parquet/Postgres recovery tests and the full `cdf-project` suite.

## Limits

This does not add CLI `resume` or `replay package` wiring. It also does not add exact named Parquet/Postgres lifecycle failpoints; the no-receipt replay tests prove source-free artifact replay, while DuckDB conformance still owns exact named crash-window failpoints.
