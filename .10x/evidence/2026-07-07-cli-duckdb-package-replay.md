Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/2026-07-07-cli-replay-package-spine.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# CLI DuckDB Package Replay Evidence

## What was observed

`cdf replay package <DIR> --to duckdb://path` now parses, rejects missing `--to`, replays DuckDB package artifacts without source contact, commits a checkpoint in the selected environment state store, records a `replay_recorded` run-ledger event, and reports package hash, destination id, target, receipt id, checkpoint id/status, receipt source duplicate/no-op status, and package status.

Focused CLI tests also observed that Postgres, Parquet, unknown destination schemes, and missing package artifacts fail closed before replay mutation. The Postgres test used a file-backed secret reference and confirmed the resolved secret value was not emitted.

## Procedure

- `cargo fmt --all`: passed.
- `cargo test -p cdf-cli --locked replay_package -- --nocapture`: passed, 7 replay-focused tests.
- `cargo test -p cdf-project --locked artifact_replay -- --nocapture`: passed, 6 artifact replay tests.
- `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`: passed.
- `cargo test -p cdf-cli --locked -- --nocapture`: passed, 77 unit tests, 1 integration test, and 0 doc tests.
- `cargo check --workspace --all-targets --locked`: passed.
- `git diff --check -- . ':(exclude).gitignore'`: passed.
- `cargo hack check -p cdf-cli -p cdf-project --all-targets --each-feature --locked`: passed.
- `cargo deny check`: passed; advisory, ban, license, and source checks were ok, with non-failing duplicate-version warnings.
- `cargo audit`: passed with one allowed warning, `RUSTSEC-2024-0436` for `paste`, matching the active scoped exception.
- `cargo vet --locked`: passed, `Vetting Succeeded (420 exempted)`.
- `osv-scanner scan source --lockfile Cargo.lock --format json --output-file target/quality/reports/osv-cli-duckdb-package-replay.json .`: exited 1 with exactly `RUSTSEC-2024-0436` for `paste`, matching the active scoped exception and no unratified advisory.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-cli-duckdb-package-replay.json crates/cdf-cli/src crates/cdf-project/src`: passed, 0 findings across 19 tracked files.
- `rg -n "unsafe|extern \"|raw pointer|Send|Sync" crates/cdf-cli/src crates/cdf-project/src`: no matches.
- `gitleaks protect --staged --redact --no-banner --report-format json --report-path target/quality/reports/gitleaks-staged-cli-duckdb-package-replay.json`: passed, no leaks found.

## What this supports

This supports the DuckDB-only slice of `.10x/tickets/2026-07-07-cli-replay-package-spine.md`:

- Parser requires `--to`.
- DuckDB replay uses package artifacts and does not contact source files after package creation.
- Duplicate DuckDB replay reports duplicate/no-op status.
- Unsupported destination schemes fail before state or destination mutation.
- Missing package artifacts fail before DuckDB destination parent or checkpoint state creation.
- Postgres and Parquet remain fail-closed at the CLI boundary.

## Limits

This does not enable Postgres package replay because target and merge dedup policy CLI inputs remain unratified.

This does not enable filesystem Parquet package replay because CLI destination URI spelling remains unratified by active records.

CodeQL was not run for this focused DuckDB-only CLI replay slice. The reusable database at `target/quality/codeql-db-rust` would need a source-fingerprint refresh after these Rust edits; this evidence therefore does not claim current-tree CodeQL coverage.
