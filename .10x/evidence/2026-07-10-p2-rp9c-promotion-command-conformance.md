Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-rp9c-promotion-command-concurrency-conformance.md

# RP9C promotion command and concurrency conformance

## What was observed

The schema-promotion command executes through generic destination runtime preparation for DuckDB, live Postgres, and Parquet correction sidecars. Multi-target execution uses canonical target order and one exact checkpoint chain, survives a later-target crash after source-package deletion, and publishes only the exact committed target set.

Every persisted execution boundary writes an append-only create-or-verify recovery event containing its phase, target state, remaining action, and exact recovery command. JSON and human failures render this status. Resolved destination credentials are absent from generated artifacts and both output modes.

SQLite settlement and ordinary checkpoint commits share transactional promotion authority. Promotion settlement is lease-fenced; ordinary run checkpoints carrying a stale schema hash are rejected after publication for that resource. Newer publications for unrelated resources do not mask the relevant authority. The v3 run ledger migrates to v4 without losing prior events and accepts new publication records.

## Procedure

- `cargo fmt --all -- --check`
- `cargo test -p cdf-state-sqlite --lib` — 38 passed
- `cargo test -p cdf-dest-postgres --lib` — 40 passed, including live local Postgres cases
- `cargo test -p cdf-project --lib` — 163 passed
- `cargo test -p cdf-cli --lib` — 258 passed, including live Postgres promotion
- `cargo clippy -p cdf-state-sqlite -p cdf-dest-postgres -p cdf-project -p cdf-cli --all-targets -- -D warnings`
- `git diff --check`

Focused regression owners include:

- `schema_promote_multi_target_uses_canonical_checkpoint_chain_and_exact_publication`
- `schema_promote_execute_recovers_every_persisted_crash_boundary`
- `schema_promote_failure_reports_persisted_recovery_status_without_secret_leak`
- `schema_promote_execute_commits_correction_checkpoint_lock_and_idempotent_publication`
- `schema_promote_execute_updates_postgres_through_generic_command_dispatch`
- `schema_promote_execute_routes_parquet_through_correction_sidecar`
- `sqlite_checkpoint_commit_rejects_schema_stale_after_promotion_publication`
- `sqlite_scope_lease_persists_fence_across_reopen`
- `scope_lease_acquire_is_exclusive_under_concurrent_contention`
- `sqlite_state_migration_upgrades_v3_without_losing_events_and_enables_publication`
- project lock CAS contention and stale-authority tests in `cdf-project`

## What this supports

This supports every RP9C acceptance criterion: structured recovery output, multi-target committed-prefix recovery and exact publication, command-level destination coverage, promotion/pin/run conflict fencing, secret redaction, and additive legacy migration.

## Limits

The Postgres scenario uses an ephemeral local server when the toolchain is available or `TEST_DATABASE_URL` when supplied. Recovery journal events are deliberately derived evidence rather than commit authority; an operating-system kill between an authoritative mutation and its journal append may leave the last event conservative, but replay derives truth from packages, receipts, checkpoints, the lock, and publication and remains safe.
