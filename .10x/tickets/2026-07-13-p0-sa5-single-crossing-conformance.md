Status: open
Created: 2026-07-13
Updated: 2026-07-13
Parent: .10x/tickets/2026-07-13-p0-single-crossing-schema-admission.md
Depends-On: .10x/tickets/2026-07-13-p0-sa2-metadata-inventory-observation-cache.md, .10x/tickets/2026-07-13-p0-sa3-fused-codec-admission.md, .10x/tickets/2026-07-13-p0-sa4-dynamic-producer-admission.md

# P0 SA5: single-crossing conformance closure

## Scope

Prove the single-crossing law across source archetypes, sampled/exhaustive coverage, cache states, preview/run, retry/replay, and residual/quarantine outcomes.

## Non-goals

No implementation repair beyond closure findings.

## Acceptance criteria

- Transport/process counters prove the governing scenarios and distinguish metadata probes from payload transfers.
- Preview/run share admission semantics and do not duplicate source execution.
- Jobs 1/N, cache hit/miss, and retry/replay retain deterministic package identity.
- Adversarial review passes with every finding resolved or durably accepted.

## References

- `.10x/specs/single-crossing-schema-admission.md`

## Assumptions

None beyond referenced completed children.

## Journal

Pending.

## Blockers

Depends on SA2-SA4.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.

