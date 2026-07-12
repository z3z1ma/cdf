Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p3-ws-v-vectorized-validation.md
Depends-On: .10x/tickets/done/2026-07-11-p3-v2-validation-graph-integration.md, .10x/tickets/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md

# P3 V3: validation envelope and conformance closeout

## Scope

Run the complete hot-rule/type/density/batch matrix, enforce ≥1 GB/s/core, publish roofline/profile/allocation evidence, make regression gates permanent in the appropriate benchmark tier, and close WS-V.

## Acceptance criteria

- Ratified 64k matrix reaches target without counting uninspected bytes.
- Vector/scalar property/fuzz and end-to-end semantics remain green.
- Kernel, evidence materialization, and end-to-end costs are separately visible.
- Regression gate is variance-aware and absent from fast checks.

## Evidence expectations

Raw host reports/profiles/counters, roofline ratios, correctness corpus, generated envelope, CI tier proof, and adversarial workload/performance review.

## Explicit exclusions

No target weakening without a superseding decision.

## Blockers

Blocked on V2 and C4.

## References

- `.10x/specs/vectorized-contract-validation.md`
