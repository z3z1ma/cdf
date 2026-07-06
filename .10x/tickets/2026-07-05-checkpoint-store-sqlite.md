Status: open
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md

# Implement checkpoint store and SQLite ledger

## Scope

Implement the `CheckpointStore` trait contract, in-memory store, SQLite WAL store, checkpoint schema, propose/commit/abandon/history/rewind operations, state-versioned position serialization, and receipt-gated firn-line invariant. Owns checkpoint trait extensions in `firn-kernel` only when needed and `crates/firn-state-sqlite/**`.

## Acceptance criteria

- No committed checkpoint can be written except by commit with a receipt covering package hash and represented segments.
- SQLite enforces one committed head per pipeline/resource/scope transactionally.
- Rewind appends history or markers, never deletes transitions, and reports packages ahead of state.
- Position serialization round-trips for all required position shapes.

## Evidence expectations

Record unit/integration tests for commit rejection without receipt, concurrent head behavior, rewind, position migrations, and SQLite WAL schema.

## Explicit exclusions

No destination mirror recovery implementation unless needed for store tests; no CLI.

## Progress and notes

- 2026-07-05: Opened from book and specs.

## Blockers

None.

