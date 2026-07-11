Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p3-ws-l-performance-lab.md
Depends-On: .10x/specs/performance-lab-and-envelope.md, .10x/specs/runtime-event-spine.md

# P3 WS-L2: phase duration and byte telemetry

## Scope

Add additive, non-artifact event facts sufficient to measure decode, validation/normalization, segment encode, persist/hash, destination write, finalize/receipt, and checkpoint gate durations and bytes. Preserve P1 rendering/redaction and keep clocks outside deterministic package identity.

## Acceptance criteria

- Kernel event types carry additive optional phase duration/byte facts without importing runtime/engine/product types.
- Runtime emits balanced begin/end or completed phase evidence on success and names interrupted/error phases honestly.
- JSON event compatibility is additive; existing consumers and P1 snapshots remain green.
- Timing collection can be disabled or rate-limited without changing execution semantics or artifacts.
- Tests prove secret redaction and that identical runs retain identical package hashes with telemetry enabled/disabled.

## Evidence expectations

Event schema tests, runtime phase fixture, hash-invariance test, renderer snapshots, generated reference freshness, and architecture review.

## Explicit exclusions

No Tokio migration, stage concurrency, memory ledger, or optimization.

## Blockers

None after P1 closure. Coordinate changes with the existing event-spine schema rather than creating a benchmark-only channel.
