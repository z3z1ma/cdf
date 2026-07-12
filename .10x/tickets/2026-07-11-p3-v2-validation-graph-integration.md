Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p3-ws-v-vectorized-validation.md
Depends-On: .10x/tickets/done/2026-07-11-p3-v1-vector-kernel-plan.md, .10x/tickets/2026-07-11-p3-a5-streaming-operator-graph.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md

# P3 V2: validation masks and selected evidence in the graph

## Scope

Integrate vector masks/aggregates with normalize/filter/package/quarantine/residual paths, fuse only where measured, remove duplicate scalar evaluation, ledger-account selected-row evidence, and preserve package/verdict bytes.

## Acceptance criteria

- Ordinary production validation uses V1 kernels once per batch.
- Accepted rows remain vectorized; only required selected rows materialize evidence.
- High-failure evidence spills/bounds correctly and no row/verdict disappears.
- Existing and new golden packages/jobs invariance/crash recovery pass.

## Evidence expectations

Integration/golden/chaos/memory tests, duplicate-evaluation static/profile proof, phase telemetry, before/after macro profiles, and adversarial evidence review.

## Explicit exclusions

No semantic rule additions or target closeout.

## Blockers

Blocked on V1, A5, and A2.

## References

- `.10x/specs/vectorized-contract-validation.md`

## Progress and notes

- 2026-07-11: Production preview and run contract execution now use a run-lived `VectorValidationEvaluator`; it binds once per observed Arrow schema and automatically rebinds when physical provenance metadata changes. Both fused no-residual and residual-present paths use the same vector evaluator. The scalar function remains only as the `cdf-contract` oracle/property surface, and an engine architecture test forbids calling it from production execution. All 90 non-ignored engine tests passed. The existing 64k fused-transform benchmark improved from the recorded 3.912 GiB/s baseline to 15.658 GiB/s at the same 200 iterations; unfused measured 2.183 GiB/s and fused/unfused outputs remain golden-identical. V2 remains open for exact ledger accounting/spill of high-failure selected evidence and macro package/TLC profiles.
