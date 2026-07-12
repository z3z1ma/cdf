Status: done
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/done/2026-07-11-p3-a5-streaming-operator-graph.md
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

## Progress and notes

- 2026-07-11: Added an explicit nonidentity fused/unfused execution control and permanent package-level conformance. The control uses the same semantic graph/plan authority; both modes produced identical engine output, package hash/signature, canonical segments, quarantine Parquet bytes, verdict/evidence artifacts, lineage, and positions for a domain-quarantine plus residual-capture schema.
- 2026-07-11: Implemented the first fused hot-path specialization for batches without pre-contract residual candidates. It retains the vector contract evaluator and Arrow bitmap filtering but eliminates the unfused row-sized residual acceptance vector, variant vector, source-to-output map, grouping map, rule-summary map, and redundant residual filter construction. Release measurement on 64k-row mixed int/string/bool batches over 200 iterations improved from 1.426 GiB/s to 3.912 GiB/s (2.743x). Full engine tests passed (82, with three prior explicit stress/performance tests ignored) and strict Clippy passed. Memory ownership, high-cardinality evidence sinks, residual-present fusion, and cancellation publication remain open.
- 2026-07-11: Transform output/scratch now reserves from the shared ledger before contract/residual allocation. The reservation conservatively covers input/output plus worst-case JSON expansion, reconciles to actual normalized Arrow bytes, transfers with sliced batches through canonical segment assembly, and releases only after durable segment persistence. A normal run records transform-class peak usage and returns current bytes to zero; a 64-byte budget fails cleanly with a `Data` error before allocation and also returns to zero. Full engine tests pass (84 with four explicit stress/performance tests ignored) and strict Clippy passes. Dedup-spool replay accounting and detailed evidence cardinality remain open.
- 2026-07-11: Closed detailed evidence cardinality. Quarantine summaries derive deterministic part paths into an atomic streaming artifact instead of retaining a path vector. Residual decisions are sorted in bounded per-batch runs under the shared spill budget, merged with fan-in 32 under an 8 MiB managed-memory lease, and streamed into the canonical contract-evolution artifact. Managed and compatibility executions produce identical manifest identity/hash; spill and memory current bytes return to zero.
- 2026-07-11: Closure gate: all 84 non-ignored engine tests passed, four explicitly labeled stress/performance tests remained opt-in, and strict all-target Clippy passed. The adversarial review at `.10x/reviews/2026-07-11-p3-a5b-fused-transform-review.md` passed. Intermediate external-sort disk amplification and package-finalizer cardinality are owned by E2 rather than hidden in this ticket.
