Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-a6-spillable-package-dedup.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md

# Host-injected shared spill budget

## What was observed

`ExecutionServices` now exposes one host-owned `SpillBudgetCoordinator`. Its reservations atomically enforce a process-wide disk-byte ceiling across operators, support checked incremental growth, record peak/current/failure telemetry, and release through RAII on every return/unwind path. The standalone host installs the accepted 8 GiB default through a named `cdf-memory` constant; embedders can inject another implementation or budget without changing an operator.

## Procedure

- `cargo test -p cdf-runtime spill::tests -- --nocapture` — passed shared exhaustion, growth, peak, failure, and drop/reacquire assertions.
- `cargo check -p cdf-runtime -p cdf-engine` — passed after host/service integration.

## What this supports

A6 can reserve scratch growth before writing and report clean exhaustion without maintaining a private per-operator disk counter. Future sort, transport spool, and metadata spill operators share the same neutral authority.

## Limits

This is byte-budget authority, not a scratch filesystem manager. A6 still owns opaque owner-only paths, typed spill identities, idempotent cleanup, and the bounded external dedup algorithm.
