Status: open
Created: 2026-07-13
Updated: 2026-07-13
Parent: .10x/tickets/2026-07-13-p0-single-crossing-schema-admission.md
Depends-On: .10x/tickets/2026-07-11-p3-g1-streaming-transport-byte-sources.md

# P0 SA2: metadata-only inventory and observation cache

## Scope

Make file inventory payload-free and add a versioned observation cache keyed by immutable generation plus codec/options/normalizer/contract identity.

## Non-goals

No fused decoder changes or dynamic producer lifecycle.

## Acceptance criteria

- Local/object-store/HTTP inventory reads no payload bytes.
- `sample_files` selection occurs before any probe for every registered format.
- Cache exact hits avoid schema I/O; weak/mismatched/corrupt entries miss safely.
- Cache storage, bounds, cleanup, and telemetry are explicit and remain outside package identity.

## References

- `.10x/specs/single-crossing-schema-admission.md`
- `.10x/specs/sampled-schema-discovery-coverage.md`

## Assumptions

Cache keys and authority limits are fixed by the governing spec.

## Journal

Pending.

## Blockers

Depends on G1 generation-bound identities.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.

