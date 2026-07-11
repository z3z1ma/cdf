Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p0-destination-extension-boundary.md
Depends-On: .10x/tickets/2026-07-11-p0-dx1-neutral-runtime-crate.md

# P0 DX2: driver-owned adapters and composition root

## Scope

Move DuckDB, Parquet, and Postgres runtime driver adapters into their destination crates; build one explicit CLI first-party registry; inject it into project entry points; remove builtin registration and production convenience constructors from shared runtime code.

## Acceptance criteria

- Destination-specific planning/private types remain inside destination crates.
- `cdf-project` removes all `cdf-dest-*` Cargo dependencies and imports.
- CLI composition is one auditable registration list; generic commands receive registry authority.
- Existing run/replay/resume/promotion artifacts and receipts remain stable.

## Blockers

Depends on DX1.
