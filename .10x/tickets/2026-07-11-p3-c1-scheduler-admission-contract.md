Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-c-deterministic-parallelism.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/2026-07-11-p0-sx1-source-extension-boundary.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/specs/deterministic-parallel-scheduler.md

# P3 C1: scheduler capabilities and hierarchical admission

## Scope

Add canonical partition/unit ordinals, extended scheduling capabilities, global auto/jobs resolution, fair hierarchical admission, CPU/native slots, shared source rate/quota, and checkpoint-scope leases. Establish deterministic test scheduler and static no-private-pool gates without production fan-out.

## Acceptance criteria

- Plans reject missing/unsafe working-set/retry/speculation/concurrency declarations.
- Effective jobs joins container CPU, memory, source, transport, destination, lane, scope, and configured ceilings and is observable/nonidentity.
- Mock sources/destinations/native lanes cannot oversubscribe or starve under adversarial admission tests.
- No scheduler branch names a first-party source/destination/library.
- Deterministic test host reproduces admission/cancellation scenarios.

## Evidence expectations

Capability serialization, admission/deadlock/fairness/lease tests, container quota fixtures, static pool gates, Loom where practical, CPU/context-switch benchmark, and adversarial review.

## Explicit exclusions

No production partition fan-out/reorder or distributed scheduling.

## Blockers

Depends on L5, SX1, A2, and A4.

## References

- `.10x/decisions/canonical-frontier-parallel-scheduling.md`
- `.10x/research/2026-07-11-deterministic-parallel-scheduler-audit.md`
- `.10x/specs/deterministic-parallel-scheduler.md`
