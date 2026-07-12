Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md, .10x/specs/vectorized-contract-validation.md

# P3 WS-V: vectorized contract validation

## Scope

Replace scalar hot-path contract evaluation with engine-neutral Arrow vector kernels and bitmap verdict algebra, integrate selected outcomes into the fused graph, and enforce the ≥1 GB/s/core correctness-preserving envelope.

## Activated children

- `.10x/tickets/done/2026-07-11-p3-v1-vector-kernel-plan.md`
- `.10x/tickets/2026-07-11-p3-v2-validation-graph-integration.md`
- `.10x/tickets/2026-07-11-p3-v3-validation-envelope-closeout.md`

## Acceptance criteria

- Supported native rules have no scalar per-row production hot path.
- Vector/scalar outcomes are property/fuzz equivalent across admitted Arrow semantics.
- Failure evidence remains total/bounded and accepted rows avoid scalar materialization.
- The declared 64k matrix reaches ≥1 GB/s/core with raw roofline/evidence.

## Blockers

Blocked on L5 baseline for implementation/optimization.

## References

- `.10x/decisions/vectorized-bitmap-validation.md`
- `.10x/research/2026-07-11-validation-kernel-performance-audit.md`
- `.10x/specs/vectorized-contract-validation.md`

## Progress and notes

- 2026-07-11: V1 closed with an engine-neutral schema-bound bitmap plan, scalar differential oracle coverage, 1.73 GiB/s mixed-kernel throughput, and 8.96x full-evaluation speedup. V2 now owns replacement of the production scalar evaluator and ledger/spill integration for selected evidence.
