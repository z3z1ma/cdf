Status: done
Created: 2026-07-06
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-observability-doctor-status-sql.md
Depends-On: .10x/tickets/done/2026-07-05-duckdb-destination.md, .10x/tickets/done/2026-07-06-local-system-sql.md

# Implement DuckDB ledger and mirror drift doctor check

## Scope

Implement the first concrete `cdf doctor` ledger/destination drift check for local DuckDB projects. The probe must compare the local SQLite checkpoint ledger with DuckDB `_cdf_loads` and `_cdf_state` mirror tables using only read-only inspection.

## Acceptance criteria

- `cdf doctor` no longer reports `ledger_destination_drift` as unsupported for a `duckdb://` destination when both the SQLite state database and DuckDB database already exist.
- The drift probe must not create a missing SQLite state database, a missing DuckDB database, or missing mirror tables.
- If either local database is absent, the check reports `skipped` with an actionable reason and `doctor` exits successfully when no other check failed.
- For each committed ledger head row (`status = 'committed'` and `is_head = 1`) with a receipt, the probe verifies:
  - `_cdf_loads` contains a row keyed by the receipt's `(target, idempotency_token)` with matching `package_hash`, `receipt_id`, and stored receipt JSON.
  - `_cdf_state` contains one row for every state segment keyed by `(target, package_hash, segment_id)`.
  - Mirror segment rows match the ledger segment `scope_json`, `output_position_json`, `row_count`, and `byte_count`.
- Mirror load or state rows in the same DuckDB database that cannot be reconciled to a committed local ledger head are reported as drift.
- A clean fixture reports `passed`; a missing or mismatched load mirror, missing or mismatched state mirror, or unreconciled extra mirror row reports `failed` and makes `doctor` exit nonzero.
- JSON output includes enough structured counts or examples to identify which ledger or mirror row drifted without printing secrets.

## Evidence expectations

Record integration tests for clean, skipped, and drift cases; include commands and exit codes. Record that missing local databases were not created by the skipped probe.

## Explicit exclusions

No Postgres live drift implementation. No destination recovery, checkpoint mutation, receipt repair, or mirror-table creation. No changes to destination commit behavior beyond minimal read-only helper APIs if needed.

## References

- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/tickets/2026-07-05-observability-doctor-status-sql.md`
- `.10x/evidence/2026-07-06-local-system-sql.md`
- `.10x/evidence/2026-07-06-duckdb-destination.md`

## Progress and notes

- 2026-07-06: Opened from the active observability parent after `cdf sql` was completed. Existing DuckDB mirror schemas and checkpoint receipt structure provide the comparison keys.
- 2026-07-06: Implemented local DuckDB ledger/mirror drift checking in `cdf doctor`. The probe gates on existing SQLite and DuckDB database files before opening, reads SQLite with `SQLITE_OPEN_READ_ONLY`, reads DuckDB mirror tables through a read-only DuckDB connection, reports skipped/passed/failed with structured counts/examples, and keeps non-DuckDB drift unsupported. Evidence recorded in `.10x/evidence/2026-07-06-duckdb-ledger-mirror-doctor-drift.md`; closure review recorded in `.10x/reviews/2026-07-06-duckdb-ledger-mirror-doctor-drift-review.md`.
- 2026-07-06: Parent ran focused integration tests and a broad `QUALITY.md` sweep, including a refreshed reusable CodeQL database at `target/quality/codeql-db-rust`. Remaining nonzero supply-chain/geiger limits are pre-existing and documented in evidence.

## Blockers

None.
