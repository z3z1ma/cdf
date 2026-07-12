Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p3-a5-streaming-operator-graph.md
Depends-On: .10x/tickets/done/2026-07-11-p3-a5a-graph-edge-contracts.md, .10x/tickets/done/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md, .10x/tickets/done/2026-07-11-p3-e1-hashing-artifact-sink.md

# P3 A5c: durable segment writer and bounded reader stream

## Scope

Connect canonical segment envelopes to the hash-while-write package sink, emit downstream authority only after durability, and introduce one bounded verified durable-segment reader shape shared by run, replay, resume, correction, and destination ingress. Keep eager collection as test/compatibility-only and statically exclude it from production paths.

## Acceptance criteria

- Only hash-complete durable segments cross the handoff; failure before directory durability emits none.
- Reader memory is one accounted batch/segment window, independent of package cardinality.
- Run/replay/resume/correction and staged/finalized-only destination paths consume the same neutral stream contract.
- Production contains no whole-package `Vec<CommitSegment>`/`read_all_segments` equivalent.
- Tamper/cancel/crash tests preserve final-binding, receipt, and checkpoint laws.

## Evidence expectations

Static materialization scans, segment stream/tamper goldens, slow-destination backpressure, memory/RSS, crash matrix, replay parity, and writer/reader throughput.

## Explicit exclusions

No streaming manifest implementation, destination-specific bulk writer, or mmap.

## Blockers

Depends on A5a, A3, and E1.

## References

- `.10x/specs/streaming-operator-graph.md`
- `.10x/specs/streaming-destination-ingress.md`
- `.10x/specs/package-io-hashing-durability.md`
