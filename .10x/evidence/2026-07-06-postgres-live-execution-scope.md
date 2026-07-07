Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-postgres-destination.md, .10x/tickets/done/2026-07-06-postgres-live-execution.md

# Postgres live execution scope evidence

## What was observed

Local Postgres binaries are available:

- `/opt/homebrew/bin/postgres`
- `/opt/homebrew/bin/initdb`
- `/opt/homebrew/bin/pg_ctl`
- `/opt/homebrew/bin/psql`
- `/opt/homebrew/bin/pg_isready`

`docker` and `podman` are still unavailable, so live tests should use an ephemeral local Postgres process or an explicitly provided `TEST_DATABASE_URL`.

`crates/cdf-dest-postgres` currently depends only on `cdf-kernel` and `serde`. It exposes deterministic planning, SQL, receipt construction, mirror SQL, identifier validation, and source exercise hooks, but no Postgres driver dependency, no package row loader, and no live `commit_package` equivalent.

## Procedure

- Ran `command -v postgres`, `initdb`, `pg_ctl`, `psql`, `pg_isready`, `docker`, `podman`, and `brew`.
- Inspected `crates/cdf-dest-postgres/Cargo.toml`.
- Inspected `crates/cdf-dest-postgres/src/api.rs`, `plan.rs`, `dml.rs`, `ddl.rs`, `mirrors.rs`, `lib.rs`, and `tests.rs`.
- Compared the missing surface to `crates/cdf-dest-duckdb/src/api.rs` and `commit.rs`.

## What this supports or challenges

This challenges the stale environment-only blocker: the workspace now has local Postgres binaries available. It supports opening a focused child for the actual remaining blocker, which is a missing live driver-backed execution path and live integration evidence.

## Limits

No ephemeral Postgres server was started in this shaping slice. The evidence does not prove that the local binaries can run successfully or that a live commit path works.
