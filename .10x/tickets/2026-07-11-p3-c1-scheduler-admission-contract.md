Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-c-deterministic-parallelism.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/decisions/scheduler-source-boundary-readiness.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/specs/deterministic-parallel-scheduler.md

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

L5, A2, and A4 are complete. The scheduler-facing subset of SX1 is evidenced and ratified in `.10x/decisions/scheduler-source-boundary-readiness.md`; SX1 remains open for compiler/discovery/product hooks that are outside C1.

## References

- `.10x/decisions/canonical-frontier-parallel-scheduling.md`
- `.10x/research/2026-07-11-deterministic-parallel-scheduler-audit.md`
- `.10x/specs/deterministic-parallel-scheduler.md`

## Progress and notes

- 2026-07-11: Added serializable canonical partition/unit ordinals and `CanonicalPartitionSchedule::compile`, which validates source/scan authority, rejects duplicate partition ids, derives per-partition immutable identity hashes from driver/physical/partition authority, and records working-set/executor/retry/speculation/order semantics.
- 2026-07-11: Added container CPU authority parsing and auto-jobs resolution joining partition count, user ceiling, source maximum/useful concurrency, CPU/native slot cost, managed memory, transport connections, destination writers, blocking lane, and checkpoint scope. A single working set larger than managed memory fails cleanly with remediation.
- 2026-07-11: Added deterministic fair hierarchical admission for jobs, memory, CPU, I/O, connections, shared quota authority, and exclusive scope leases. Round-robin resource queues remain work-conserving around blocked heads; typed permits prevent double release. Production fan-out remains explicitly outside C1.
- 2026-07-11: Bound neutral source plans into CLI-built engine plans. Executable file/REST/Postgres plans now serialize the canonical partition schedule into both plan authority and explain evidence before run; Python/foreign producers remain an explicit capability migration item rather than receiving guessed declarations.
