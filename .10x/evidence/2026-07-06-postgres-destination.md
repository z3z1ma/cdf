Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/2026-07-05-postgres-destination.md, .10x/specs/destination-receipts-guarantees.md, .10x/specs/package-lifecycle-determinism.md, .10x/specs/project-cli-observability-security.md

# Postgres destination implementation evidence

## What was observed

`crates/firn-dest-postgres` now implements a deterministic Postgres destination planning surface:

- Postgres destination sheet with append, replace, and merge support; atomic package transactions; package-token idempotency; identifier rules; quarantine support; bulk path declarations; and exact/widening/lossy/unsupported Postgres-specific type mappings.
- Identifier validation and quoting that enforces Postgres' 63-byte identifier limit, rejects NUL, and reserves `_firn_*` for framework columns.
- Dry-runnable transactional DDL and DML plans for system tables, target table creation, safe existing-table nullable-column additions, append, transactional truncate-insert replace, and merge.
- Merge SQL uses explicit `MergeDedupPolicy::{First, Last, Fail}` so the destination applies the contract-provided dedup behavior instead of inventing one. First/last use `ROW_NUMBER()` over merge keys with deterministic segment/row ordering; fail emits a duplicate-key guard expected to return zero rows before `ON CONFLICT`.
- Receipt construction records Postgres transaction metadata with xid, segment acknowledgements, counts, schema hash, migrations, and a `postgres_sql` verify clause against `_firn_loads`.
- `_firn_loads` and `_firn_state` mirror DDL/upsert SQL plus doctor/project drift probe SQL hooks are exposed for later project/doctor integration.
- Postgres source-side exercise SQL hooks expose deterministic snapshot count/page and optional cursor page templates for fixture/source validation against the same server.

No live Postgres integration test was run. `pg_isready` reported `/tmp:5432 - no response`; `TEST_DATABASE_URL` and `DATABASE_URL` were unset; `docker` was not installed. This ticket therefore implemented and tested the deterministic planning, SQL, and receipt surface only.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn` after the final implementation:

```text
cargo fmt -p firn-dest-postgres
```

Result: passed.

```text
cargo test -p firn-dest-postgres --locked --no-fail-fast
```

Result: passed. Nine unit tests passed and doc tests ran zero tests. Tests cover sheet fidelity, identifier safety, append/replace/merge transactional SQL, explicit merge dedup policies, xid-bearing receipts and verify clauses, mirror/drift SQL hooks, source exercise SQL hooks, safe existing-table migrations, merge key requirements, and existing primary-key drift rejection.

```text
cargo clippy -p firn-dest-postgres --all-targets --locked -- -D warnings
```

Result: passed.

```text
git diff --check
```

Result: passed.

```text
cargo audit
cargo deny check advisories
```

Initial result: failed on two advisory findings for `pyo3 0.28.3`: `RUSTSEC-2026-0176` and `RUSTSEC-2026-0177`. The dependency path reported by `cargo deny` was through concurrent `firn-python` work, not `firn-dest-postgres`.

Parent integration revalidation after the Python dependency change passed:

```text
cargo test -p firn-project -p firn-dest-postgres -p firn-dest-duckdb -p firn-python --locked --no-fail-fast
cargo clippy -p firn-project -p firn-dest-postgres -p firn-dest-duckdb -p firn-python --all-targets --locked -- -D warnings
cargo fmt --all -- --check
cargo audit --json > target/quality/reports/cargo-audit-current-batch.json
cargo deny check advisories
osv-scanner scan source -r . --format json --output-file target/quality/reports/osv-current-batch.json
git diff --check
```

Live Postgres remains unavailable: `pg_isready` reports `/tmp:5432 - no response`, `docker` is not installed, and `TEST_DATABASE_URL`/`DATABASE_URL` are unset.

## What this supports or challenges

This supports the deterministic implementation portion of the Postgres destination ticket: the crate compiles, lints, formats, and tests its sheet, SQL planning, receipt, idempotency, and mirror hooks.

This challenges ticket closure because no live Postgres execution evidence exists. The Postgres crate did not add a live Postgres driver dependency and currently proves deterministic planning/SQL/receipt behavior only.

## Limits

The evidence does not prove execution against a live Postgres server, COPY/binary staging behavior, transactional rollback behavior, physical row counts returned by a driver, or project/doctor invocation. Those require either a reachable Postgres service or a test container and the later project/doctor integration surface.
