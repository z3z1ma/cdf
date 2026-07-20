Status: open
Created: 2026-07-05
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/2026-07-05-conformance-chaos-golden.md, .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md

# Implement distributed execution and remote state stores

## Scope

Implement distributed execution after local conformance: partition leases, worker scheduler, shared checkpoint store with lease fencing, package directories on object storage, Postgres/object-store-backed store implementations, and Ballista evaluation record if used or rejected.

## Acceptance criteria

- `(resource, partition)` is the distribution unit.
- Remote store implementations preserve the same store conformance contract and commit-gate invariant.
- Distributed workers pass resource and destination conformance suites unchanged.
- Lease fencing prevents double-commit for one scope.
- Packages remain shuffle-free handoff artifacts.

## Evidence expectations

Record store conformance output, distributed integration tests, lease-fencing tests, and a research/decision record for Ballista substrate evaluation.

## Explicit exclusions

No new artifact type may be introduced without superseding decision.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-10: `.10x/tickets/done/2026-07-10-p2-rp4-schema-scope-lease-lock-cas.md` now owns the first executor-neutral fenced `ScopeKey` lease primitive required by schema promotion, with in-memory/SQLite implementations only. This ticket MUST reuse/extend that conformance-tested primitive for remote stores and worker scheduling rather than introducing a second lease model.
- 2026-07-11: P3/P0 foundation `.10x/specs/portable-partition-task-protocol.md`, WX1, and C5 now own the neutral canonical task/result protocol and local isolated-worker equivalence law. This distributed parent MUST wrap that protocol with transport, remote stores/leases/artifacts, placement, and framework/substrate adapters; it MUST NOT create a competing worker plan or let Spark/Flink/Ballista reinterpret CDF semantics.
- 2026-07-20: `.10x/decisions/partition-separable-isolated-segment-canonicalization.md` makes the current partition capsule exact for compiler-proven separable work and explicitly rejects multi-partition package-global selectors before side effects. This ticket owns any later typed global-operator/epoch task needed to distribute those graphs; it MUST bind the compiled whole-plan authority and MUST NOT overload a partition task or reinterpret global state as partition-local.

## Blockers

None.
