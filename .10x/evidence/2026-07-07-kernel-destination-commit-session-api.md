Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-kernel-destination-commit-session-api.md, .10x/decisions/run-ledger-commit-session-spine.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/destination-receipts-guarantees.md

# Kernel destination commit-session API evidence

## What was observed

`cdf-kernel` now exposes a driver-neutral `CommitSession` trait and an additive `DestinationProtocol::begin` default method. Existing `sheet` and `plan_commit` methods remain unchanged. The default `begin` returns a destination error for destinations not yet refactored onto sessions.

Kernel tests include a fake destination/session proving:

- default `begin` fails closed with an unsupported commit-session error
- a destination can begin a session from a request and plan
- migrations run before write in the fake session
- `finalize` returns a concrete `Receipt`
- the returned receipt covers the state delta
- `abort` is available as a terminal session operation

No DuckDB, Parquet, Postgres, project runtime, CLI, run ledger, package fixture, manifest, or lockfile change was made.

## Procedure

Parent-observed checks:

- `cargo fmt --all -- --check` exited 0.
- `git diff --check` exited 0.
- `cargo test -p cdf-kernel --locked --no-fail-fast` exited 0: 10 unit tests passed; 0 doc tests.
- `cargo nextest run -p cdf-kernel --locked` exited 0: 10 tests passed.
- `cargo clippy -p cdf-kernel --all-targets --locked -- -D warnings` exited 0.
- `cargo check --workspace --all-targets --locked` exited 0.
- `cargo semver-checks -p cdf-kernel --baseline-rev HEAD` exited 0: 196 checks passed, 57 skipped, no semver update required.
- `cargo semver-checks -p cdf-kernel` exited 1 because `cdf-kernel` is not published to crates.io; the local `HEAD` baseline above is the applicable compatibility measurement.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` exited 0.
- `cargo test --workspace --all-targets --locked --no-fail-fast` exited 0 across workspace unit/integration tests, including live Postgres tests.
- `cargo doc --workspace --no-deps --locked` exited 0.
- `cargo audit` exited 0, reporting only allowed warning `RUSTSEC-2024-0436` for `paste 1.0.15`.
- `cargo deny check advisories` exited 0.
- `cargo deny check` exited 0 with non-failing duplicate-version warnings from the current Arrow/DataFusion graph.
- `cargo vet --locked` exited 0: vetting succeeded with the existing exemption backlog.
- `osv-scanner --lockfile Cargo.lock` exited 1 only for `RUSTSEC-2024-0436` / `paste 1.0.15`, which is covered by `.10x/tickets/done/2026-07-06-native-arrow-parquet-policy.md`.
- `semgrep scan --config p/rust --error crates/cdf-kernel/src` exited 0 with 0 findings.
- `gitleaks dir --no-banner --redact --log-level warn crates/cdf-kernel` exited 0.
- `gitleaks dir --no-banner --redact --log-level warn .10x/tickets/done/2026-07-07-kernel-destination-commit-session-api.md` exited 0.
- `cargo machete --with-metadata` exited 0 with no unused dependencies found.
- `rg -n "\bunsafe\b|extern \"|\*const|\*mut|impl Send|impl Sync" crates/cdf-kernel/src` exited 0 with no matches.

Skipped or limited checks:

- CodeQL was skipped per the active goal instruction to skip CodeQL for now and avoid recreating the database.
- `cargo mutants` was skipped because the active goal asks to avoid frequent mutation runs and prefer mutation testing after larger chunks; this ticket is an additive API slice with focused tests.
- `cargo geiger` was skipped because `.10x/knowledge/quality-gate-execution.md` records local Geiger cost/cleanup risk; the direct first-party unsafe scan found no owned unsafe surface in `cdf-kernel/src`.
- An initial `gitleaks dir` invocation with multiple target paths was interrupted after hanging; the command accepts one path, and the subsequent single-path scans passed.

## What this supports

This supports the ticket acceptance criteria that the kernel exposes a commit-session API, preserves existing `sheet`/`plan_commit` behavior, models migration/write/finalize/abort phases, makes `finalize` return a durable `Receipt` or error, measures public API compatibility with semver tooling, and does not change concrete destination behavior.

The broad workspace checks support that existing destination implementers still compile through the default unsupported `begin` path.

## Limits

This evidence does not prove DuckDB, Parquet, or Postgres session implementations. Those remain owned by their destination-specific child tickets.

This evidence does not prove a general run orchestrator, run ledger, CLI routing, or receipt-verification call sequencing. This ticket's API cannot advance checkpoints because it returns only `Receipt`; checkpoint advancement remains owned by `CheckpointStore::commit`.
