Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-run-spine-implementation-program.md
Depends-On: .10x/decisions/run-ledger-commit-session-spine.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# Implement the SQLite run ledger store

## Scope

Add the first project-owned append-only run ledger store for local environments.

Owns the lowest coherent storage/API slice, likely across:

- `crates/cdf-kernel/src/ids.rs` only if additional typed ids are needed.
- `crates/cdf-project/**` or `crates/cdf-state-sqlite/**` depending on where the run store trait and SQLite implementation fit best after inspection.
- Tests for event ordering, collision behavior, redaction-safe serialization, and recovery queries.

## Acceptance criteria

- The runtime can mint or accept a `RunId` and fail closed on caller-supplied id collision.
- Run events are append-only with monotonic per-run sequence numbers.
- Required event families from `.10x/specs/run-orchestration-ledger.md` can be serialized with secret references only.
- The run ledger cannot write committed checkpoints or bypass `CheckpointStore::commit`.
- Query APIs support `inspect run` and `resume` callers without requiring source contact.
- Schema versioning/migration story is recorded if new SQLite tables are introduced.

## Evidence expectations

Run focused project/state tests, workspace check, clippy for touched crates, direct redaction tests, and `git diff --check`.

## Explicit exclusions

No general orchestrator, no destination sessions, no CLI command wiring beyond test helpers, no non-SQLite backend, no distributed leases, no package format changes unless a run association artifact is explicitly scoped.

## Blockers

None.

## Progress and notes

- 2026-07-07: Implemented the first SQLite run ledger slice in `crates/cdf-state-sqlite`: run creation mints opaque SQLite-random `RunId`s or accepts caller-supplied ids with fail-closed collision behavior; run events are append-only rows with per-run monotonic sequence numbers; query APIs return run snapshots and event pointers for future `inspect run` and `resume` callers. New SQLite run tables are versioned through `cdf_sqlite_schema_migrations` with `component = 'run_ledger'` and `version = 1`; this implementation fails closed on unsupported future run-ledger schema versions rather than silently reading them.
- 2026-07-07: Parent review added hardening tests proving `cdf_runs` is append-only below the Rust API and unsupported future run-ledger schema versions fail closed.
- 2026-07-07: Verification passed: `cargo fmt --all -- --check`; `git diff --check`; `cargo test -p cdf-state-sqlite --locked --no-fail-fast` with 25 unit tests and 0 doc tests; `cargo nextest run -p cdf-state-sqlite --locked`; `cargo clippy -p cdf-state-sqlite --all-targets --locked -- -D warnings`; `cargo semver-checks -p cdf-state-sqlite --baseline-rev HEAD`; `cargo hack check -p cdf-state-sqlite --all-targets --each-feature --locked`; `cargo check --workspace --all-targets --locked`; `cargo clippy --workspace --all-targets --locked -- -D warnings`; `cargo test --workspace --all-targets --locked --no-fail-fast`; `cargo doc --workspace --no-deps --locked`; `cargo deny check`; `cargo audit`; `cargo vet --locked`; `osv-scanner --lockfile Cargo.lock` with only the ratified `RUSTSEC-2024-0436` finding; `semgrep scan --config p/rust --error crates/cdf-state-sqlite/src`; `cargo machete --with-metadata`; targeted `gitleaks`; direct unsafe scan. Evidence: `.10x/evidence/2026-07-07-sqlite-run-ledger-store.md`. Review: `.10x/reviews/2026-07-07-sqlite-run-ledger-store-review.md`.
