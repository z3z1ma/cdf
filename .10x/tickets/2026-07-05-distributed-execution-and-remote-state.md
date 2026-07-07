Status: open
Created: 2026-07-05
Updated: 2026-07-05
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/2026-07-05-conformance-chaos-golden.md, .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md

# Implement distributed execution and remote state stores

## Scope

Implement distributed execution after local conformance: partition leases, worker scheduler, shared checkpoint store with lease fencing, package directories on object storage, Postgres/object-store-backed store implementations, and Ballista evaluation record if used or rejected.

## Acceptance criteria

- `(resource, partition)` is the distribution unit.
- Remote store implementations preserve the same store conformance contract and cdf-line invariant.
- Distributed workers pass resource and destination conformance suites unchanged.
- Lease fencing prevents double-commit for one scope.
- Packages remain shuffle-free handoff artifacts.

## Evidence expectations

Record store conformance output, distributed integration tests, lease-fencing tests, and a research/decision record for Ballista substrate evaluation.

## Explicit exclusions

No new artifact type may be introduced without superseding decision.

## Progress and notes

- 2026-07-05: Opened from book and specs.

## Blockers

None.
