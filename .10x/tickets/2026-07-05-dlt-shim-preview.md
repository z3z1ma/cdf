Status: open
Created: 2026-07-05
Updated: 2026-07-05
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-python-sdk-bridge.md, .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md

# Implement dlt shim preview

## Scope

Implement preview support for running feasible `@dlt.resource` and `@dlt.source` functions through firn's Python bridge, mapping dlt hints and state to firn descriptors, contracts, and ledger views. Owns dlt-specific modules under Python bridge/SDK areas.

## Acceptance criteria

- dlt primary key, merge key, incremental, write disposition, and contract-mode hints map to firn descriptors/contracts where feasible.
- `dlt.current.state` maps to a scoped ledger-backed state view.
- Divergences from dlt behavior are documented as migration-table data or generated docs.
- Shim output is planned, packaged, and checkpointed like native firn resources.

## Evidence expectations

Record integration tests with representative dlt resources and mapping snapshots.

## Explicit exclusions

Bug-for-bug dlt emulation and dlt destination delegation are excluded.

## Progress and notes

- 2026-07-05: Opened from book and specs.

## Blockers

None.
