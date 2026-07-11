Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Streaming operator graph audit

## Question

What end-to-end operator graph removes input/package-sized materialization without leaking source or destination identities into orchestration, and which current barriers require explicit designs rather than ordinary channels?

## Sources and methods

Inspected engine extraction/package execution, package builder/reader, run replay, destination commit sessions, contract/dedup execution, active memory/segmentation/staged-ingress/runtime specifications, and P3 tickets. Traced batch, quarantine, segment, source-position, and acknowledgement ownership from `ResourceStream::open` through package finalization and destination commit.

## Findings

The current engine is sequential by partition and batch. It performs schema checks, transform/contract evaluation, quarantine writes, normalization, and IPC persistence in one loop. Ordinary accepted batches are written promptly, but package-scoped keyed or exact-row dedup stores every accepted `RecordBatch` in `pending_dedup_batches` and evaluates after all partitions. Segment identity is a global encounter-order counter.

Package persistence writes a complete IPC file, then rereads it to calculate its file entry/hash. Package readers expose a streaming-looking segment unit, but `read_commit_segments` builds and returns a `Vec<CommitSegment>` containing every requested segment and decoded batch. DuckDB's convenience commit path uses that eager collection. This makes replay/commit memory scale with package size.

Quarantine and residual facts are emitted alongside accepted data but are written synchronously as small artifacts. Evidence/profile/lineage/segment descriptors accumulate in vectors/maps until finalization. Even after batch memory is bounded, those control structures can scale with file, observation, batch, or segment count. Constant-memory claims therefore require bounded aggregation and append/spill-backed canonical metadata construction, not only byte-bounded Arrow channels.

The semantic graph has two kinds of stages:

- streaming local stages: partition read/decode, schema reconciliation, transform/contract/normalize, canonical segment assembly, segment encode/persist/hash, and optional destination staging;
- stateful barriers: package-order dedup and package finalization/final destination binding.

Inserting an async channel between every logical transform would add allocation, scheduling, and permit churn. Schema reconciliation, contract evaluation, residual capture, normalization, and projection operate on the same batch and can execute as one fused CPU kernel while preserving separate plan/verdict identities.

Pre-finalization destination overlap is legal only through the staged-ingress contract. Finalized-only destinations begin after package verification. In both cases, replay and run should consume the same bounded durable-segment stream abstraction; run must not have a privileged direct-to-destination path.

## Conclusion

Compile each resource transition into a typed, capability-driven operator graph. Use accounted envelopes and bounded edges only where concurrency or ownership transfer is useful; fuse adjacent stateless CPU transforms. Make the durable, hash-complete package segment the single source/destination handoff. Package metadata/evidence uses append/spill-backed canonical sinks with bounded summaries.

Package-global dedup is an explicit ordered spillable barrier with its own child ticket, not a hidden vector in the transform loop. The graph can overlap independent partitions before that barrier and segment persistence/destination staging after it while canonical order remains plan-derived.

## Limits

This audit does not choose exact edge capacities, fusion thresholds, dedup partitioning, manifest draft storage format, or stage concurrency. L5 and focused child tickets must measure and ratify those implementation details without altering the graph semantics.
