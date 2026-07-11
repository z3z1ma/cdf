Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Vectorized bitmap validation

## Context

The P3 envelope requires contract validation at ≥1 GB/s/core on 64k-row batches. The current per-rule/per-row evaluator and row outcome construction cannot meet that goal predictably and puts correctness logic on a scalar hot path.

## Decision

`cdf-contract` owns an executor-neutral compiled vector validation plan over Arrow arrays. Supported type/null/domain/range rules evaluate into bitmaps/selection masks using typed Arrow kernels or specialized safe Rust kernels selected from measurement. The total verdict lattice combines masks in canonical rule order/precedence and produces accepted/quarantine/residual/drop/error selections plus aggregate counts.

Schema/type checks and array downcasts happen once during batch-plan binding, never per row. Rules on the same column may share typed bindings/scans when semantics remain explicit. Accepted rows are not converted to scalar values. Detailed violation/residual/quarantine values are materialized only for selected rows and retain rule/provenance identity.

The current scalar evaluator becomes a correctness oracle for property/fuzz/differential testing. It is not an automatic production fallback for supported hot rules. A rule without a proven vector implementation is rejected/declared non-vectorized at plan time with explicit performance classification; custom interpreted rules execute through declared foreign lanes and cannot be blended into native validation claims.

`cdf-engine` consumes the neutral masks/outcomes and may fuse selection/projection with adjacent operators, but DataFusion does not own contract meaning. Package/verdict bytes and row order remain unchanged unless a separately ratified artifact migration is required.

## Alternatives considered

- Keep scalar and parallelize batches: rejected because it wastes cores on avoidable branches/conversions and misses per-core target.
- Express all rules only as DataFusion predicates: rejected because contract semantics become engine-owned.
- Generate SIMD/unsafe kernels immediately: rejected until safe Arrow kernels/reference evidence show a gap; unsafe still requires focused decision/fuzz.
- Collect one outcome object per row: rejected because all-pass workloads should retain bitmap/aggregate form.

## Consequences

V1 owns the compiled kernel plan and differential oracle. V2 integrates masks/evidence into the fused graph. V3 owns the performance/correctness matrix and target. Rule additions must declare vectorization/performance class and conformance.
