Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-duckdb-commit-session-refactor.md, .10x/tickets/done/2026-07-07-parquet-commit-session-refactor.md, .10x/tickets/done/2026-07-07-postgres-commit-session-refactor.md, .10x/specs/destination-receipts-guarantees.md

# Destination commit-session refactors

## What was observed

DuckDB, Parquet/object-store, and Postgres destinations now expose `DestinationProtocol::begin` implementations backed by `CommitSession` while preserving their existing package-commit facades. Focused tests cover successful session finalization, duplicate/idempotent replay behavior, and abort-before-write behavior where meaningful.

The implementation intentionally preserves the existing durable write semantics of each destination. DuckDB and Parquet sessions delegate `write` to the prior durable package commit path and return the already durable receipt from `finalize`. Postgres keeps system DDL, duplicate lookup, target write, mirrors, receipt verification, and final transaction commit inside one explicit session.

## Procedure

Local verification commands observed for this slice:

- `cargo fmt --all -- --check` passed.
- `git diff --check` passed.
- `cargo check --workspace --all-targets --locked` passed.
- `cargo test -p cdf-dest-duckdb --locked --no-fail-fast` passed with 11 tests.
- `cargo test -p cdf-dest-parquet --locked --no-fail-fast` passed with 18 tests.
- `cargo test -p cdf-dest-postgres --locked --no-fail-fast` passed with 27 tests, including live local Postgres session tests.
- `cargo test -p cdf-project --locked --no-fail-fast` passed with 35 tests.
- `cargo clippy -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres --all-targets --locked -- -D warnings` passed.
- `cargo nextest run -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres --locked` passed with 56 tests.
- `cargo semver-checks -p cdf-dest-duckdb --baseline-rev HEAD` passed.
- `cargo semver-checks -p cdf-dest-parquet --baseline-rev HEAD` passed.
- `cargo semver-checks -p cdf-dest-postgres --baseline-rev HEAD` passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` passed.
- `cargo doc --workspace --no-deps --locked` passed.
- `cargo deny check` passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast` passed.
- `cargo audit` exited 0 with only the ratified allowed `RUSTSEC-2024-0436` / `paste 1.0.15` warning.
- `cargo vet --locked` passed.
- `osv-scanner --lockfile Cargo.lock` exited 1 only for the ratified `RUSTSEC-2024-0436` / `paste 1.0.15` finding.
- `semgrep scan --config p/rust --error crates/cdf-dest-duckdb/src crates/cdf-dest-parquet/src crates/cdf-dest-postgres/src` passed with 0 findings.
- `gitleaks dir --no-banner --redact --log-level warn crates/cdf-dest-duckdb` passed.
- `gitleaks dir --no-banner --redact --log-level warn crates/cdf-dest-parquet` passed.
- `gitleaks dir --no-banner --redact --log-level warn crates/cdf-dest-postgres` passed.
- `cargo machete --with-metadata` found no unused dependencies.
- `cargo hack check -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres --all-targets --each-feature --locked` passed.
- Direct unsafe scan over the changed destination crate source found no Rust unsafe constructs; the only match was the literal string `"unsafe"` in an existing Postgres source-runtime predicate test fixture.

CodeQL was intentionally skipped because the active execution instruction is to avoid recreating the CodeQL database. Mutation testing was deferred because the active quality instruction is to reserve it for larger chunks. Geiger was not run because `.10x/knowledge/quality-gate-execution.md` records the current local cost/risk; the direct unsafe scan above was used for this slice.

## What this supports

This supports closing the three destination child tickets as implemented and verified:

- `.10x/tickets/done/2026-07-07-duckdb-commit-session-refactor.md`
- `.10x/tickets/done/2026-07-07-parquet-commit-session-refactor.md`
- `.10x/tickets/done/2026-07-07-postgres-commit-session-refactor.md`

It also supports unblocking `.10x/tickets/2026-07-07-general-run-orchestrator.md` on destination commit-session implementations, leaving the run-ledger store as the remaining declared dependency.

## Limits

This evidence does not prove a general project orchestrator exists. DuckDB and Parquet still need package context captured by their package-aware planning APIs, and Postgres uses `with_commit_request` as a compatibility handoff. That input-shape constraint is owned by `.10x/tickets/2026-07-07-general-run-orchestrator.md`.

This evidence also does not claim streaming/restartable writes within a destination session. DuckDB and Parquet preserve their previous durable package-commit granularity, and Postgres preserves one explicit transaction around the existing live commit behavior.
