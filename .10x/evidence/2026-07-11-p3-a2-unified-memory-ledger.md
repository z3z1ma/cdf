Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/specs/runtime-memory-backpressure.md, .10x/decisions/runtime-memory-ledger-byte-permits.md

# P3 A2 unified memory ledger evidence

## What was observed

The new `cdf-memory` crate provides implementation-neutral typed consumers, borrowing sub-caps, RAII leases, shared accounted Arrow/byte envelopes, async weighted admission, operator working-set declarations, budget resolution, and JSON-reportable telemetry. It has no Tokio, DataFusion, project, engine, runtime, or destination dependency.

`cdf-engine` owns the default DataFusion adapter. CDF reservations and DataFusion consumers compete against the exact same finite pool. DataFusion release wakes CDF waiters, and snapshots attribute current/peak bytes to both CDF and named query-engine consumers. Multi-file discovery now acquires weighted `discovery.metadata` permits before probe I/O and enforces both byte and worker limits while restoring canonical result order.

## Procedure

- `cargo test -p cdf-memory --locked` — 9 passed.
- `cargo test -p cdf-engine --lib --locked` — 54 passed.
- `cargo test -p cdf-project --lib --locked` — 172 passed.
- `cargo clippy -p cdf-memory -p cdf-engine -p cdf-project --all-targets --locked -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.

Focused laws cover shared-clone single charging, allocation reconciliation, constructor error cleanup, panic unwind, pending-future cancellation, weighted wakeup, undeclared sub-cap failure, minimum-working-set rejection, non-pausable spill declaration, external source/destination profiles, understated retained memory, shared DataFusion competition, DataFusion attribution, and deterministic discovery concurrency.

## What this supports

This supports all A2 criteria: one finite byte authority, neutral accounted payloads, memory-before-job admission, real discovery limits, reportable process/headroom/managed and consumer facts, extension isolation, and working-set falsification.

## Limits

A2 establishes the authority and contracts before wholesale operator migration. A5/A6/SX1 and destination children own conversion of each production queue, decoder, dedup table, package writer, and destination staging buffer. WS-F remains the independent RSS/process-tree proof.
