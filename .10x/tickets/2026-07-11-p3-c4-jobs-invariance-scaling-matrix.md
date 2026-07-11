Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-c-deterministic-parallelism.md
Depends-On: .10x/tickets/2026-07-11-p3-c3-engine-ffi-parallel-integration.md

# P3 C4: jobs-invariance and scaling matrix

## Scope

Make jobs 1/N invariance permanent across source/format/destination archetypes, run scaling to each roofline under skew/failure/limit/rate/scope constraints, and close local parallelism triage with measured evidence.

## Acceptance criteria

- Every permanent archetype produces identical semantic artifacts/receipt identities at jobs 1/2/auto/N.
- Scaling continues until a named hardware/network/destination limit; scheduler overhead and speculative waste are bounded.
- Oversubscription, starvation, slow-frontier, scope conflicts, and single-writer cases remain green.
- The local-partition-parallelism triage closes into evidence/no-action items.

## Evidence expectations

Generated invariance matrix/hashes, host scaling curves/profiles, stress/chaos output, triage reconciliation, and adversarial skew review.

## Explicit exclusions

No distributed scheduler.

## Blockers

Depends on C1-C3.

## References

- `.10x/tickets/2026-07-07-local-partition-parallelism-triage.md`
- `.10x/specs/deterministic-parallel-scheduler.md`
