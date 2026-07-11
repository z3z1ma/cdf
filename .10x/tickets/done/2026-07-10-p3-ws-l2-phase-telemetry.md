Status: done
Created: 2026-07-10
Updated: 2026-07-11
Parent: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md
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

None. Coordinate changes with the existing event-spine schema rather than creating a benchmark-only channel.

## Progress and notes

- 2026-07-11: Added kernel-owned, typed terminal phase metrics for decode, validation/normalization, segment encode, persist/hash, package finalize, destination write/receipt, checkpoint gate, and aggregate package execution. The engine exposes telemetry through a general execution-options value rather than a benchmark-only or source-specific path.
- 2026-07-11: Runtime collection is opt-in, capped at 32 terminal events by default, and creates no clocks when disabled. Failed runs close every active runtime phase with `failed` status before `run_failed`; metrics remain outside package identity.
- 2026-07-11: Added append-only run-ledger schema v5 with migration from every supported prior version; CLI inspection/progress consumers accept and redact the additive value type without changing ordinary event order or snapshots.
- 2026-07-11: Closure evidence is `.10x/evidence/2026-07-11-p3-l2-phase-telemetry.md`; architecture review is `.10x/reviews/2026-07-11-p3-l2-phase-telemetry-review.md` (pass).
