Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p3-a5-streaming-operator-graph.md
Depends-On: .10x/tickets/done/2026-07-11-p3-a5a-graph-edge-contracts.md, .10x/tickets/done/2026-07-11-p3-a6-spillable-package-dedup.md

# P3 A5b: fused transform and bounded evidence kernel

## Scope

Move schema reconciliation, projection/coercion, vector contract evaluation, residual/variant handling, normalization, and output conformance into one graph CPU fusion group with an unfused conformance mode. Carry quarantine/verdict/lineage facts causally and persist detailed evidence through bounded sinks/summaries.

## Acceptance criteria

- Fused/unfused execution produces identical accepted rows, quarantine, residual/variant values, verdicts, lineage, positions, segment bytes, and package hashes.
- Newly allocated output/scratch reserves before allocation; dropped rows cannot drop required evidence.
- The accepted-row hot path remains vectorized and contains no scalar row reconstruction.
- Evidence detail cardinality is bounded/spill-backed and cancellation cannot publish a partial success summary.
- Fusion improves or preserves throughput against the unfused measured control; no channel remains without measured overlap value.

## Evidence expectations

Full semantic golden matrix, generated chunking/pressure properties, memory and cancellation traces, vector/static gates, and fusion before/after profiles.

## Explicit exclusions

No decoder SIMD implementation, package finalizer, destination bulk encoding, or parallel scheduler.

## Blockers

Depends on A5a envelopes and A6's package-global barrier.

## References

- `.10x/specs/streaming-operator-graph.md`
- `.10x/specs/vectorized-contract-validation.md`
- `.10x/specs/spillable-package-dedup.md`
