Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-05-duckdb-destination.md
Verdict: pass

# DuckDB destination closure review

## Target

Review of `crates/cdf-dest-duckdb/**`, `.10x/evidence/2026-07-06-duckdb-destination.md`, and `.10x/tickets/done/2026-07-05-duckdb-destination.md` against `.10x/specs/destination-receipts-guarantees.md`, `.10x/specs/package-lifecycle-determinism.md`, and `.10x/specs/architecture-layering-runtime.md`.

## Findings

No closure-blocking findings.

The implementation uses the real DuckDB driver and commits through DuckDB transactions. Append, replace, and merge tests exercise actual DuckDB database files, not mocks. Package-token idempotency is enforced by `_cdf_loads` with `(target, idempotency_token)` as the primary key; duplicate replay returns the stored receipt and does not rewrite target rows.

Receipt verification is destination-backed: `verify_receipt` reopens the database and checks `_cdf_loads` for the receipt JSON keyed by target, idempotency token, and package hash. This covers the committed-before-checkpointed recovery window described by the package lifecycle spec.

The merge path avoids an unratified semantic default. Exact duplicate package rows collapse deterministically before commit; conflicting duplicate merge keys fail with a data error instead of choosing first-wins or last-wins behavior that no active record specifies.

The sheet and extended capability report are honest about current limits: direct DuckDB Arrow appender support is not used because `duckdb-rs` is Arrow 58-facing while CDF packages use Arrow 59, and Parquet replay is unsupported until package archive Parquet data exists under `.10x/tickets/done/2026-07-05-singer-airbyte-and-package-archive.md`.

## Verdict

Pass. The DuckDB destination satisfies the executable ticket's current acceptance criteria with real DuckDB integration, package replay, DDL planning, dispositions, idempotency, mirrors, verifiable receipts, single-writer locking, and ICU probing.

## Residual risk

The single-writer lock is a lockfile created with `create_new`; it prevents concurrent cdf writers under normal process exit but can leave a stale lock after a hard process kill. No separate follow-up ticket was opened in this write scope because `.10x/tickets/2026-07-05-conformance-chaos-golden.md` owns crash-matrix hardening and can decide whether stale-lock fencing is required.

The type surface is intentionally conservative. Nested types, decimals, timezone-aware timestamps, and nanosecond temporal values that would lose precision are rejected rather than coerced. Destination conformance under `.10x/tickets/2026-07-05-conformance-chaos-golden.md` should keep falsifying the sheet as new mappings are added.
