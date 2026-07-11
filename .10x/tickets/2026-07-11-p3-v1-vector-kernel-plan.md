Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p3-ws-v-vectorized-validation.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md

# P3 V1: typed vector kernel plan and scalar oracle

## Scope

Compile batch-schema-bound null/domain/range/type kernels in `cdf-contract`, implement bitmap verdict algebra/aggregates, retain scalar oracle, and add generated differential/property/fuzz coverage without engine integration.

## Acceptance criteria

- One-time typed binding removes per-row downcasts/value conversion from vector path.
- All admitted type/rule/disposition combinations match scalar oracle exactly.
- Unsupported combinations fail plan/deep validation precisely.
- All-pass and high-failure memory is measured/bounded.

## Evidence expectations

Property/fuzz corpus, oracle diffs, allocation/criterion microbench before/after, type matrix, dependency/supply-chain checks, and adversarial semantic review.

## Explicit exclusions

No engine fusion, custom foreign rules, unsafe SIMD, or artifact change.

## Blockers

Blocked on L5.

## References

- `.10x/specs/vectorized-contract-validation.md`
