Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/done/2026-07-05-postgres-destination.md

# Implement live Postgres destination execution

## Scope

Implement the live execution slice missing from `crates/firn-dest-postgres`: a driver-backed package commit path that consumes the existing deterministic `PostgresLoadPlan`, loads package rows into a Postgres staging table, executes append/replace/merge transactions, records `_firn_loads` and `_firn_state`, builds xid-bearing receipts, verifies receipts against Postgres, and proves the path against an ephemeral local Postgres server.

The worker owns `crates/firn-dest-postgres/**`, any required dependency additions for the Postgres driver, focused integration-test helpers, and this ticket's evidence/review records. It may touch workspace lockfiles only as required by dependency changes.

## Acceptance criteria

- The crate exposes a live commit API analogous to the existing DuckDB/Parquet package commit APIs, using `PostgresLoadPlan` rather than inventing a parallel planning model.
- The live path reads canonical package IPC segments, validates requested segment coverage, maps package schema to the existing Postgres column plan, and stages rows with `_firn_load`, `_firn_segment`, `_firn_row`, and `_firn_loaded_at_ms` values.
- Append, transactional replace, and merge execute against Postgres with deterministic dedup behavior matching the existing plan semantics.
- Replaying the same package against the same target returns duplicate/no-op behavior from `_firn_loads` and does not rewrite target rows.
- Receipts include xid metadata, segment acknowledgements, counts, schema hash, migrations, idempotency token, and a `postgres_sql` verify clause that succeeds against the live server.
- `_firn_loads` and `_firn_state` are populated transactionally with the target write when state delta metadata is present.
- Rollback/fail-closed behavior is covered by a test that injects or triggers an error inside the transaction and proves no partial target/mirror state is committed.
- Integration tests run against an ephemeral local Postgres instance started from available `postgres`/`initdb`/`pg_ctl` binaries, or against `TEST_DATABASE_URL` when explicitly provided. Tests MUST NOT require Docker.
- The implementation preserves the crate organization convention in `.10x/knowledge/rust-crate-organization.md`; do not collapse the crate back into a monolithic `lib.rs`.

## Evidence expectations

Record focused unit tests, live integration tests against Postgres for append/replace/merge/duplicate/receipt verification/rollback/mirror state, and the relevant `QUALITY.md` gates. Run independent checks in parallel where safe. Reuse `target/quality/codeql-db-rust` through the existing stale-aware wrapper and do not recreate the CodeQL database unnecessarily.

## Explicit exclusions

No Postgres source connector, no CLI command wiring, no project/doctor drift UI, no non-SQLite checkpoint store backend, no warehouse destination, no CDC/log-source implementation, and no broad conformance parent closure.

## Progress and notes

- 2026-07-06: Opened after rechecking the parent blocker. Local Homebrew Postgres binaries are present, but the current crate has no driver dependency or live commit path. The worker should start from the existing `PostgresLoadPlan`/SQL/receipt surface and keep the live API small.
- 2026-07-06: Implemented and parent-reviewed live Postgres package commits using the existing `PostgresLoadPlan`, canonical package loading, COPY staging, append/replace/merge transactions, xid receipts, schema-scoped mirrors, duplicate no-op replay, receipt verification, Decimal128/Decimal256 mapping, and best-effort package receipt append reporting. Evidence recorded in `.10x/evidence/2026-07-06-postgres-live-execution.md`; review recorded in `.10x/reviews/2026-07-06-postgres-live-execution-review.md`.

## Blockers

None.
