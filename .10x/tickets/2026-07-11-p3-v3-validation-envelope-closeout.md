Status: active
Created: 2026-07-11
Updated: 2026-07-15
Parent: .10x/tickets/2026-07-11-p3-ws-v-vectorized-validation.md
Depends-On: .10x/tickets/done/2026-07-11-p3-v2-validation-graph-integration.md, .10x/tickets/done/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md

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

None. V2 and C4 are complete.

## Journal

- 2026-07-19: Activated after F1 closure because V3 is dependency-ready and converts an already-proven production hot path into a permanent performance law without changing runtime semantics. The existing ignored `cdf-contract` tests prove a 64k mixed kernel and scalar ratio but do not represent the ratified batch/type/density matrix, emit machine-readable host evidence, or enforce a variance-aware threshold. The implementation slice will live in `cdf-benchmarks`, keep timing out of fast checks, count only bytes each rule actually inspects, and retain boundary/evidence-materialization cells as visible non-throughput claims rather than inflating the ≥1 GB/s/core kernel gate.

## References

- `.10x/specs/vectorized-contract-validation.md`
