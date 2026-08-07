Status: open
Created: 2026-08-07
Updated: 2026-08-07
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/2026-08-07-a1-5-package-native-keyed-effects.md`

# A6.1: shared-upstream fan-out execution

## Scope

Implement a compiled project execution graph with physical-upstream signatures, compatible-group
projection union, one accounted read/decode stream, bounded zero-copy branch fan-out, per-resource
package/destination/checkpoint branches, all-branches shared-frontier settlement, crash recovery,
manifest/plan inspection, telemetry, and conformance.

## Non-goals

- heuristic sharing by configured-source name;
- sharing divergent initial frontiers;
- branch-specific predicate/limit/order hoisting without exact proof;
- merging resource, package, target, receipt, or checkpoint identity;
- distributed execution.

## Acceptance criteria

- [ ] Resource identity is removed from the physical upstream signature while remaining in every
      logical branch identity.
- [ ] Compatible selected resources open/read/decode once and receive exact projection-union
      branch inputs with identical standalone package bytes.
- [ ] Fan-out shares payload/leases under bounded backpressure and cancellation without deep clones
      or private unaccounted buffers.
- [ ] Shared source acknowledgement advances only after every branch receipt/checkpoint settles;
      recovery reuses durable branch packages and never double-applies committed work.
- [ ] Divergent frontiers or unproven semantics compile separate extraction groups.
- [ ] Plan/manifest/telemetry expose canonical group/signature/branch/settlement identities.
- [ ] Focused graph/project/CLI behavior, crash, jobs, and memory tests pass.

## References

- `.10x/specs/shared-upstream-fanout-execution.md`
- `.10x/research/2026-08-07-routed-target-shared-extraction-readiness.md`
- `.10x/specs/streaming-operator-graph.md`
- `.10x/specs/checkpoint-state-commit-gate.md`

## Assumptions

All execution semantics are active, user-ratified authority. No compatibility path is permitted.

## Journal

- 2026-08-07: Opened after ratification. Current `source_node_id` includes resource identity and
  CLI selection executes independent serial `ProjectRunRequest`s, so this is a project-graph
  implementation rather than a local source cache.

## Blockers

Dependency A1.5 must close before keyed CDC branch integration. Neutral row fan-out scaffolding may
begin afterward within this ticket; no separate speculative abstraction ticket is authorized.

## Evidence

Pending implementation.

## Review

Pending tranche-level review.

## Retrospective

Pending implementation.
