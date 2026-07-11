Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p3-a5-streaming-operator-graph.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/tickets/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md, .10x/specs/spillable-package-dedup.md

# P3 A6: spillable package-order dedup barrier

## Scope

Implement final-output-row dedup placement, versioned typed key equality, accounted in-memory fast path, measured bounded external winner algorithm, canonical payload/key/decision spools, ordinal rejoin, v2 sharded provenance, legacy reader compatibility, and constant-memory conformance.

## Acceptance criteria

- Exact-row identity includes residual/variant and every normalized final output field; keyed fields resolve unambiguously.
- First/last/fail semantics match the simple reference evaluator for all supported Arrow types and random rechunking.
- Forced memory-to-spill transition at every boundary produces byte-identical package/evidence to the in-memory path.
- Adversarial high-cardinality/skew/collision inputs remain within budget and do not lose or merge unequal keys.
- No output/staged segment is durable before `fail` uniqueness certification.
- V2 provenance is deterministically sharded and bounded in memory; v1 packages remain inspectable/replayable.
- Scratch disk exhaustion/cancellation/crash fails cleanly and cleanup is idempotent.
- The 100 GB dedup stress completes within the configured RSS ceiling; algorithm selection has before/after/crossover evidence.

## Evidence expectations

Algorithm comparison, artifact-version decision/goldens, generated Arrow equality/reference properties, forced collisions, in-memory/spill/jobs invariance, RSS/disk stress, cleanup/crash tests, replay compatibility, security permission checks, and adversarial correctness/performance review.

## Explicit exclusions

No destination-state dedup, approximate filters as authority, cross-package retention window, distributed shuffle, or destination-specific implementation.

## Blockers

Depends on L5, unified accounting, and canonical order/segmentation. The v2 writer must not land without explicit package artifact migration evidence.

## References

- `.10x/decisions/spillable-package-order-dedup.md`
- `.10x/research/2026-07-11-package-dedup-spill-audit.md`
- `.10x/specs/spillable-package-dedup.md`
- `.10x/decisions/keyless-exact-row-deduplication.md`
- `.10x/decisions/contract-live-verdict-execution-semantics.md`
