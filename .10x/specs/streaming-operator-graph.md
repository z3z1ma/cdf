Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Streaming operator graph

## Purpose and scope

This specification governs graph compilation, semantic stages, stage fusion, accounted edges, outcome/evidence flow, durable segment handoff, bounded control metadata, error/cancellation behavior, and source/destination extension invariants. Host, memory, segmentation, staged ingress, and commit-gate details remain governed by their focused active specifications.

## Compilation contract

Each planned resource transition MUST compile a typed graph before extraction. The graph MUST record semantic node identities, ordering requirements, boundedness, minimum/maximum working sets, spillability, executor/lane class, concurrency limits, fusion groups, durable boundaries, and capability joins.

Graph construction MUST consume generic source/resource, operator, memory, package, and destination capabilities. Generic runtime code MUST NOT branch on a source kind, format name, destination name, URI scheme, or first-party crate type. Unsupported capability combinations fail during plan with the conflicting declarations and remediation.

## Data and outcome envelopes

Every data edge MUST carry an accounted envelope with partition ordinal/id, local deterministic sequence, exact source position authority, schema/coercion authority, shared or owned memory reservation, and Arrow/byte payload. Newly allocated transform output and scratch MUST reserve before allocation. Naked `RecordBatch` or `Vec<u8>` values MUST NOT cross production graph edges.

Pre-contract quarantine, residual candidates/decisions, contract verdicts, variant capture, lineage, and profile deltas MUST remain causally attached to their originating outcome. A bounded evidence sink MUST persist detailed facts and maintain bounded summaries. Dropping data cannot drop its required evidence; cancellation cannot report a partial success summary.

## Fusion and concurrency

Schema reconciliation, projection/coercion, contract evaluation, residual/variant handling, normalization, and compatible transforms SHOULD run as one fused CPU kernel when they share an executor/lane and no stateful/durable boundary intervenes. Fusion MUST preserve individual plan/verdict identities and produce identical accepted rows, quarantine, lineage, and package bytes to an unfused conformance execution.

Channels/queues MUST exist only at useful ownership or concurrency boundaries and MUST be byte-accounted through the unified ledger. Queue item-count limits MAY defend metadata cardinality but do not replace byte accounting. Concurrency is admitted through the injected execution host and cannot exceed memory, CPU, source, destination, ordering, or single-writer capabilities.

## Stateful barriers and ordering

Package-order dedup and any future package-global operator MUST declare an ordered stateful barrier. The barrier MUST consume canonical partition/row order, spill under pressure, and emit jobs/batch/pressure-invariant results. It MUST NOT retain all input batches or keys in unaccounted memory.

Parallel outputs MUST enter a memory-accounted reorder boundary keyed only by plan partition ordinal and local deterministic sequence. Reorder pressure backpressures faster partitions. Scheduler completion order cannot determine canonical segments, evidence order, source positions, or package identity.

## Package and destination boundary

Canonical segment assembly emits bounded segment contents to a writer that encodes, persists atomically, and calculates SHA-256 while writing. A segment becomes eligible for downstream handoff only after durable bytes and identity are complete.

Production package reading MUST expose a bounded segment stream/iterator that verifies one segment at a time under a memory lease. Eager whole-package collections are compatibility/test helpers and MUST NOT be called by run, replay, resume, correction, or destination bulk paths.

Staged-ingress destinations MAY accept durable segments before finalization. Finalized-only destinations begin after verified package finalization. Both MUST consume the same durable-segment stream shape and driver contract. Final binding, receipt verification, and checkpoint commit remain serialized by the commit gate.

## Bounded metadata and finalization

Segment/file entries, observation/coercion facts, quarantine indexes, lineage, acknowledgements, and event/profile details MUST use append-only or spill-backed canonical draft sinks when their cardinality can grow with input. Resident summaries, reorder indexes, and draft writer buffers MUST have explicit ledger bounds. Final package artifacts MUST stream in canonical order from the drafts and remain byte-identical across jobs, channel pressure, spill timing, and destination speed.

Telemetry with wall time, queue pressure, executor scheduling, adaptive microbatch boundaries, or staging attempt identity MUST stay outside package identity. Identity-bearing evidence may not depend on task completion order.

## Failure and recovery

First terminal failure stops admission and cancels the structured run scope. Every operator MUST release or transfer memory, CPU, lane, spill, temporary-file, staged-ingress, and draft-sink ownership before the scope returns. No package finalization, final destination binding, receipt, or checkpoint may occur after an upstream required outcome failed.

Durable completed segments remain ordinary recoverable package drafts; staged destination state follows staged-ingress abort/reattach rules. Retry/replay reads durable segments through the same bounded path and never requires re-materializing the source.

## Conformance and performance

Permanent conformance MUST compare fused/unfused execution; jobs 1/N; tiny/large/nested batches; slow source/destination; quarantine/residual/variant outcomes; limit; partition failure; cancellation at every edge; spill; reordered completion; finalized-only and staged destinations; and replay. Package hash, segments, positions, verdicts, lineage, and committed receipt identities MUST be invariant where semantics require.

The lab MUST report per-node and per-edge bytes, rows, service/wait time, queue depth/bytes, CPU utilization, spill, reorder window, persistence/hash throughput, destination overlap, and fusion overhead. Measurement events MUST be rate-limited and outside the hot identity path. The ordinary graph SHOULD add no channel whose measured overlap benefit does not exceed its cost.

## Explicit exclusions

This spec does not define distributed scheduling, remote worker exchange, exact dedup spill algorithm, manifest draft storage engine, decoder internals, destination-specific bulk encoding, or dynamic plugin loading.
