Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/done/2026-07-11-p3-a1-staged-ingress-final-binding.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/tickets/done/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/specs/streaming-operator-graph.md

# P3 A5: compiled fused streaming operator graph

## Activated execution tickets

- `.10x/tickets/done/2026-07-11-p3-a5a-graph-edge-contracts.md`
- `.10x/tickets/done/2026-07-11-p3-a5b-fused-transform-kernel.md`
- `.10x/tickets/done/2026-07-11-p3-a5c-durable-segment-stream.md`
- `.10x/tickets/done/2026-07-11-p3-e2-streaming-manifest-durability.md` (cross-workstream bounded metadata/finalization owner)
- `.10x/tickets/2026-07-11-p3-a5e-streaming-graph-integration.md`
- `.10x/tickets/done/2026-07-11-p3-a6-spillable-package-dedup.md`

## Scope

Implement the typed graph compiler, fused stateless CPU kernel chain, accounted ownership-transfer edges, deterministic reorder boundary, bounded outcome/evidence flow, durable segment writer/reader stream, bounded metadata draft sinks, and staged/finalized destination integration. Remove production whole-package segment collection and route run/replay/resume through the same bounded durable-segment path.

Split implementation into bounded children for graph/edge contracts, fused transform execution, segment persistence/reader, metadata draft sinks, and end-to-end integration before execution begins; this parent is not executable.

## Acceptance criteria

- Generic graph construction contains no source/format/destination-name branch; mock external drivers compose through capabilities.
- Fused and unfused conformance produce identical package/evidence/verdict results.
- Slow source/destination and out-of-order partitions remain bounded and propagate backpressure.
- Production run/replay/resume/destination paths contain no whole-package `Vec<CommitSegment>` or equivalent materialization.
- Detailed evidence and manifest cardinality remain bounded/spill-backed for very large file/segment counts.
- Only durable hash-complete segments reach destinations; staged and finalized-only paths preserve final binding and commit-gate laws.
- Cancellation/failure at every edge leaves no tasks, permits, temporary drafts, or unowned staged state.
- Before/after lab evidence shows useful overlap and records channel/fusion overhead.

## Evidence expectations

Static architecture tests, mock-driver capability composition, fused/unfused and jobs invariance goldens, byte/RSS stress, high-cardinality metadata stress, slow-stage/backpressure tests, crash/cancellation matrix, replay parity, benchmark profiles, and adversarial performance/architecture review.

## Explicit exclusions

No distributed scheduler, remote worker protocol, exact production dedup spill algorithm, decoder-specific SIMD work, or destination-specific bulk encoder.

## Blockers

Depends on the pre-optimization baseline and A1 through A4 contracts/implementations. A dedicated dedup/spill child must land before A5 can claim package-global constant memory.

## References

- `.10x/decisions/compiled-fused-streaming-operator-graph.md`
- `.10x/research/2026-07-11-streaming-operator-graph-audit.md`
- `.10x/specs/execution-host-structured-runtime.md`
- `.10x/specs/runtime-memory-backpressure.md`
- `.10x/specs/canonical-segmentation-adaptive-batching.md`
- `.10x/specs/streaming-destination-ingress.md`
- `.10x/tickets/done/2026-07-11-p3-a6-spillable-package-dedup.md`
