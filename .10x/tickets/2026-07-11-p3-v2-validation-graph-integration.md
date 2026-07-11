Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p3-ws-v-vectorized-validation.md
Depends-On: .10x/tickets/2026-07-11-p3-v1-vector-kernel-plan.md, .10x/tickets/2026-07-11-p3-a5-streaming-operator-graph.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md

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
