Status: done
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

## Progress and notes

- 2026-07-11: Added a schema-bound, engine-neutral vector plan in `cdf-contract`. Field resolution, Arrow type selection, regex compilation, and domain/range literal preparation happen once; batch evaluation produces packed per-rule, accepted, and quarantine masks with canonical summaries. Null/domain/range/type coverage spans every type admitted by the scalar evaluator, including canonical numeric-domain spelling, timestamp units, NaN and signed-zero identity. Regex and freshness use prebound typed kernels rather than a scalar fallback. Selected quarantine values are materialized only after masks identify violating rows. Differential proptest plus signed/unsigned/float/string/timestamp/null fixed matrices match the scalar oracle exactly; all-failure mask storage is bit-packed. Release measurements on 64k rows report 1.73 GiB/s for the mixed null/range/string-domain mask kernel and 8.96x full-evaluation speedup over the scalar oracle. Evidence: `.10x/evidence/2026-07-11-p3-v1-vector-validation-kernel.md`; review: `.10x/reviews/2026-07-11-p3-v1-vector-validation-kernel-review.md`.

## Retrospective

Typed domain compilation must preserve the scalar contract's canonical string comparison, not merely parsed numeric equality. In particular, invalid numeric literals are impossible matches rather than plan errors, while NaN and signed zero require explicit identity handling. Bitmap evaluation and selected evidence materialization are separate APIs so the all-pass path never constructs row objects.
