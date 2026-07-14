Status: open
Created: 2026-07-13
Updated: 2026-07-13
Parent: .10x/tickets/2026-07-13-p0-fixed-schema-discovery-stream-admission.md
Depends-On: .10x/tickets/done/2026-07-13-p0-sa0-cold-discovery-final-plan-lifecycle.md, .10x/tickets/done/2026-07-13-p0-sa1-compiled-stream-admission-plan.md, .10x/tickets/2026-07-13-p0-sa2-metadata-inventory-observation-cache.md, .10x/tickets/2026-07-13-p0-sa3-fused-codec-admission.md, .10x/tickets/2026-07-13-p0-sa4-dynamic-producer-admission.md

# P0 SA5: fixed-schema discovery/admission conformance closure

## Scope

Prove cold-freeze and pinned-stream-admission laws across source archetypes, both coverage axes, cache/spool states, preview/run, retry/replay, and residual/quarantine outcomes.

## Non-goals

No implementation repair beyond closure findings.

## Acceptance criteria

- Transport/process counters distinguish inventory, bounded probes, full payload transfer, duplicate bounded bytes, and same-command spool reuse.
- Preview/run share admission semantics and do not duplicate source execution.
- Jobs 1/N, cache hit/miss, and retry/replay retain deterministic package identity.
- Adversarial review passes with every finding resolved or durably accepted.

## References

- `.10x/specs/schema-discovery-and-stream-admission.md`

## Assumptions

None beyond referenced completed children.

## Journal

Pending.

## Blockers

Depends on SA0-SA4.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
