Status: done
Created: 2026-07-05
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md

# Implement checkpoint store and SQLite ledger

## Scope

Implement the `CheckpointStore` trait contract, in-memory store, SQLite WAL store, checkpoint schema, propose/commit/abandon/history/rewind operations, state-versioned position serialization, and receipt-gated commit-gate invariant. Owns checkpoint trait extensions in `cdf-kernel` only when needed and `crates/cdf-state-sqlite/**`.

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
- 2026-07-06: Worker implemented the runtime-neutral `CheckpointStore` trait, in-memory store, SQLite WAL-backed store, receipt-gated commit path, transactional single-head SQLite invariant, append-only rewind marker behavior, and state-versioned JSON round-trip tests in `crates/cdf-state-sqlite`. Evidence recorded in `.10x/evidence/2026-07-06-checkpoint-store-sqlite.md`; the ticket then entered parent review.
- 2026-07-06: Repaired parent review blockers by moving the runtime-neutral checkpoint contract and serde-backed checkpoint artifact values into `cdf-kernel`, keeping the SQLite and in-memory implementations in `cdf-state-sqlite`, and removing the public raw SQLite connection accessor. Required repair verification passed and was appended to `.10x/evidence/2026-07-06-checkpoint-store-sqlite.md`.
- 2026-07-06: Repaired ratified book §12.5 blocker by changing the kernel `CheckpointStore` trait to `Send + Sync` with shared receivers, hiding implementation mutation behind standard-library `Mutex` synchronization in both stores, and adding a compile-time test that both stores implement the thread-safe trait. Required repair verification passed and was appended to `.10x/evidence/2026-07-06-checkpoint-store-sqlite.md`.
- 2026-07-06: Hardened checkpoint-store tests against parent mutation findings by running core conformance checks across in-memory and SQLite stores, adding tuple-isolation and rewind-validation cases, adding private SQLite row-corruption reads for delta/receipt mismatches, asserting timestamp provenance, and covering branch-lineage ahead-of-state reporting. Required verification passed and a rerun of the parent mutation command shape reported 111 mutants tested: 74 caught, 37 unviable, 0 missed.
- 2026-07-06: Parent reran the QUALITY gate set and recorded the final results in `.10x/evidence/2026-07-06-checkpoint-quality-gates.md`. Closure review `.10x/reviews/2026-07-06-checkpoint-store-sqlite-review.md` passed with repository-level supply-chain policy work kept under `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`. Acceptance criteria are satisfied and this ticket is closed.

## Blockers

None.
