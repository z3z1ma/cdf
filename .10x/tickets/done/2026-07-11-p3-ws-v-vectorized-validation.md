Status: done
Created: 2026-07-11
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md, .10x/specs/vectorized-contract-validation.md

# P3 WS-V: vectorized contract validation

## Scope

Replace scalar hot-path contract evaluation with engine-neutral Arrow vector kernels and bitmap verdict algebra, integrate selected outcomes into the fused graph, and enforce the ≥1 GB/s/core correctness-preserving envelope.

## Activated children

- `.10x/tickets/done/2026-07-11-p3-v1-vector-kernel-plan.md`
- `.10x/tickets/done/2026-07-11-p3-v2-validation-graph-integration.md`
- `.10x/tickets/done/2026-07-11-p3-v3-validation-envelope-closeout.md`

## Acceptance criteria

- Supported native rules have no scalar per-row production hot path.
- Vector/scalar outcomes are property/fuzz equivalent across admitted Arrow semantics.
- Failure evidence remains total/bounded and accepted rows avoid scalar materialization.
- The declared 64k matrix reaches ≥1 GB/s/core with raw roofline/evidence.

## Blockers

None. L5 and V1-V3 are complete.

## References

- `.10x/decisions/vectorized-bitmap-validation.md`
- `.10x/research/2026-07-11-validation-kernel-performance-audit.md`
- `.10x/specs/vectorized-contract-validation.md`

## Progress and notes

- 2026-07-11: V1 closed with an engine-neutral schema-bound bitmap plan, scalar differential oracle coverage, 1.73 GiB/s mixed-kernel throughput, and 8.96x full-evaluation speedup. V2 now owns replacement of the production scalar evaluator and ledger/spill integration for selected evidence.
- 2026-07-19: V2 removed the scalar evaluator from production execution, retained it as a differential oracle, and recorded a 15.658 GiB/s fused validation/normalization trend plus a 7.78% package-workload improvement. V3 then made the complete matrix a permanent scheduled law.
- 2026-07-19: WS-V closes with all 12 ratified EC2 64k hot-kernel cells above 1 GB/s/core (3.016-7.254 GB/s), exact inspected-byte authority, total selected-evidence counters, current full-year TLC product-path preservation, and no scalar production fallback.

## Evidence

- V1 kernel semantics and scalar differential proof: `.10x/evidence/2026-07-11-p3-v1-vector-validation-kernel.md`.
- V2 production graph integration and fused trend: `.10x/evidence/2026-07-11-p3-v2-production-vector-validation.md`.
- V2 macro workload preservation: `.10x/evidence/2026-07-11-p3-v2-smoke-macro-profile.md`.
- V3 full matrix, roofline, evidence cost, and scheduled gate: `.10x/evidence/2026-07-19-p3-v3-validation-envelope.md`.

## Review

Verdict: pass. The serialized validation program remains semantic authority; native production rules execute through schema-bound vector kernels; accepted rows avoid scalar evidence materialization; selected failures retain bounded, separately measured evidence; and the EC2 matrix exceeds the ratified threshold without byte-accounting loopholes. Residual risk is limited to unavailable hardware counter collection on the current EC2 image, not an unmeasured product behavior.

## Retrospective

The successful seam is program binding followed by engine-neutral masks: it preserves total verdict semantics while allowing execution fusion without making DataFusion or a runtime engine authoritative for identity-bearing output. The permanent lesson is to measure kernel masks, selected evidence, and complete product phases separately, because combining them creates incentives to hide expensive failure paths or credit bytes a rule never inspected.
