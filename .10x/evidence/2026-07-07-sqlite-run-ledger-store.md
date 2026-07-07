Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-run-ledger-store.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# SQLite run ledger store

## What was observed

`cdf-state-sqlite` now exports `SqliteRunLedger` and run-ledger data types for local run identity and event storage. The store can mint opaque `run-*` ids, accept caller-supplied `RunId`s, reject run-id collisions, append ordered per-run events, return per-run snapshots, and serialize event details with typed secret references.

The SQLite schema adds `cdf_runs`, `cdf_run_events`, append-only triggers for both tables, pointer indexes for checkpoint/receipt/package lookup, and a `cdf_sqlite_schema_migrations` row for `component = 'run_ledger'`, `version = 1`. Opening a database with a future run-ledger schema version fails closed.

Focused tests prove run-id minting/collision rejection, per-run monotonic sequence numbers, append-only enforcement below the Rust API, all required event kinds, secret-reference serialization and raw-secret rejection for sensitive detail keys, inspect/resume snapshot pointer shape, checkpoint-store isolation, and schema-version recording/rejection.

## Procedure

Local verification commands observed for this slice:

- `cargo fmt --all -- --check` passed.
- `git diff --check` passed.
- `cargo test -p cdf-state-sqlite --locked --no-fail-fast` passed with 25 unit tests and 0 doc tests.
- `cargo nextest run -p cdf-state-sqlite --locked` passed with 25 tests.
- `cargo clippy -p cdf-state-sqlite --all-targets --locked -- -D warnings` passed.
- `cargo semver-checks -p cdf-state-sqlite --baseline-rev HEAD` passed.
- `cargo hack check -p cdf-state-sqlite --all-targets --each-feature --locked` passed.
- `semgrep scan --config p/rust --error crates/cdf-state-sqlite/src` passed with 0 findings.
- Direct unsafe scan over `crates/cdf-state-sqlite/src` found no first-party unsafe constructs.
- `cargo check --workspace --all-targets --locked` passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast` passed.
- `cargo doc --workspace --no-deps --locked` passed.
- `cargo deny check` passed with existing non-failing duplicate-version warnings.
- `cargo audit` exited 0 with only the ratified allowed `RUSTSEC-2024-0436` / `paste 1.0.15` warning.
- `cargo vet --locked` passed.
- `osv-scanner --lockfile Cargo.lock` exited 1 only for the ratified `RUSTSEC-2024-0436` / `paste 1.0.15` finding.
- `cargo machete --with-metadata` found no unused dependencies.
- `gitleaks dir --no-banner --redact --log-level warn crates/cdf-state-sqlite` passed.
- `gitleaks dir --no-banner --redact --log-level warn .10x/tickets/done/2026-07-07-run-ledger-store.md` passed after ticket move.

CodeQL was intentionally skipped because the active execution instruction is to avoid recreating the CodeQL database. Mutation testing was deferred because the active quality instruction is to reserve it for larger chunks. Geiger was not run because `.10x/knowledge/quality-gate-execution.md` records the current local cost/risk; the direct unsafe scan above was used for this slice.

## What this supports

This supports closing `.10x/tickets/done/2026-07-07-run-ledger-store.md` and unblocking `.10x/tickets/done/2026-07-07-general-run-orchestrator.md` on the run-ledger-store dependency.

## Limits

This evidence does not prove the general orchestrator or CLI `resume`/`inspect run` wiring. The ledger exposes per-run snapshot and event queries plus indexed persisted pointers; orchestrator and CLI tickets remain responsible for defining any broader interrupted-run discovery policy and for assembling recovery actions from package, receipt, checkpoint, and ledger evidence.
