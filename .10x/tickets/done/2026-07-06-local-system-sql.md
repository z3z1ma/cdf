Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-observability-doctor-status-sql.md
Depends-On: .10x/tickets/done/2026-07-05-cli-surface.md, .10x/tickets/done/2026-07-05-package-builder-reader.md, .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md

# Implement local system SQL

## Scope

Implement the first supported `cdf sql` surface for local projects by mounting queryable, read-only system-history tables from the configured SQLite checkpoint ledger and package manifest/receipt files. Owns `crates/cdf-cli/**` and the smallest supporting APIs in `crates/cdf-state-sqlite/**` or `crates/cdf-package/**` if needed.

## Acceptance criteria

- `cdf sql <query>` loads the project and evaluates one read-only query against local system-history tables without mutating the checkpoint store, packages, destination, or project files.
- Supported tables include checkpoint history and package metadata sufficient to query package hash, lifecycle status, segments, identity files, and receipts.
- Non-read-only SQL is rejected with a usage or contract error before any persistent side effect is possible.
- JSON output is stable and includes column names plus row values.
- Human output is concise and suitable for scheduler logs.
- The CLI unsupported-surface record is updated so `sql` is no longer listed as missing once implemented.

## Evidence expectations

Record targeted tests for checkpoint/package SQL mounting, read-only rejection, JSON shape, and no-write behavior. Record targeted `cargo test -p cdf-cli --locked --no-fail-fast`, `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`, and `cargo fmt --all -- --check` output.

## Explicit exclusions

No destination mirror drift comparison, live DuckDB/Postgres mirror querying, run orchestration, package replay, status freshness evaluation, dashboard/UI, or mutation-capable SQL shell.

## Progress and notes

- 2026-07-06: Split from `.10x/tickets/2026-07-05-observability-doctor-status-sql.md` as the first bounded observability child. The goal is to remove the CLI `sql` blocker without inventing runtime commit semantics.
- 2026-07-06: First broad worker attempt was shut down without integration after it did not return a status checkpoint. Re-dispatched a narrower CLI-only worker with an explicit in-memory SQLite/system-history mounting path.
- 2026-07-06: Implemented local read-only `cdf sql` in `crates/cdf-cli/src/system_sql.rs`, mounting checkpoint rows and package manifest/receipt metadata into an in-memory SQLite database. Targeted CLI tests, formatting, and clippy pass. Evidence recorded in `.10x/evidence/2026-07-06-local-system-sql.md`; closure review recorded in `.10x/reviews/2026-07-06-local-system-sql-review.md`.

## Blockers

None.
