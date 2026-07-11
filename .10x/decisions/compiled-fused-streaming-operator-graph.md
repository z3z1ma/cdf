Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Compiled fused streaming operator graph

## Context

CDF must overlap I/O and CPU at hardware speed while holding constant memory and preserving package identity, verdicts, receipt gating, replay, and simple source/destination extension. The current engine combines all work in a sequential loop, retains package-wide dedup inputs, and replay materializes all segments. A literal channel per function would be bounded but unnecessarily expensive and would expose implementation details as architecture.

## Decision

Every resource transition compiles into one typed operator graph with these semantic roles:

1. partition source/read/decode;
2. schema reconciliation and pre-contract outcomes;
3. transform, contract, residual capture, normalization, and projection;
4. optional package-order stateful operators such as dedup;
5. canonical segment assembly;
6. segment encode, durable persist, and hash;
7. optional staged destination ingress;
8. package evidence/manifest finalization;
9. verified final destination binding or finalized-package streaming commit;
10. receipt verification and checkpoint commit through the existing gate.

The graph compiler uses source, operator, memory, execution-host, package, and destination capabilities. It MUST NOT match concrete source/destination names. A new driver declares capabilities and implements its boundary; generic graph construction does not change.

Logical roles are not mandatory thread/channel boundaries. Adjacent stateless CPU operations over the same Arrow buffers are fused into a kernel chain by default. Accounted byte-bounded edges exist only where ownership transfer enables useful I/O/CPU overlap, concurrency, affinity isolation, stateful barriers, or durable handoff. Fusion and edge placement cannot change semantic plan nodes, verdicts, lineage, canonical ordering, or evidence.

The graph's data envelope carries plan partition ordinal, local deterministic sequence/position, shared memory lease, Arrow payload, schema/coercion authority, and bounded side outcomes. Quarantine/residual/verdict facts travel with their originating data outcome until a bounded evidence sink persists or aggregates them; they are not detached unbounded side channels.

The only run-to-destination payload handoff is a durable, hash-complete canonical package segment. Staged destinations may consume that segment before package finalization under `LoadAttemptId`; finalized-only destinations consume the same segment-reader stream after finalization. Replay uses the same reader/driver path. Direct unverified source-to-destination writes are forbidden.

Package-wide ordered operations are explicit barriers. They must be spillable and deterministic and cannot retain a package-sized `Vec<RecordBatch>`. Stateful barrier semantics and required ordering are visible in the compiled graph and memory plan.

Control-plane cardinality is also budgeted. Per-row/batch/file/segment facts are appended to canonical bounded sinks or spill-backed draft indexes; only bounded summaries and working windows remain resident. Final manifest/evidence serialization streams deterministically from those drafts. Constant memory includes Arrow payloads, metadata, telemetry, acknowledgements, reorder state, and native working sets.

Backpressure propagates from destination/staging through durable persistence and upstream edges. Package persistence remains authoritative: a slow or failed destination cannot cause source bytes to bypass the package or alter canonical segments. Operators follow their owner-declared flush, release/backpressure, spill, clean-fail sequence.

## Alternatives considered

- One monolithic async loop: rejected because it cannot overlap resource classes and encourages package-sized locals.
- One task/channel per function: rejected because fixed scheduling/allocation overhead would compromise row-oriented throughput.
- Direct source-to-destination streaming with evidence on the side: rejected because it bypasses the package artifact and replay authority.
- Destination-specific graph templates: rejected because adding a destination would require orchestration edits.
- Keep metadata in memory because it is smaller than data: rejected because file/segment cardinality grows without a useful upper bound.

## Consequences

The runtime needs a graph compiler, fused kernel executor, accounted envelope/edge abstraction, bounded evidence/manifest sinks, and a bounded durable-segment stream. Existing eager package reader APIs become compatibility helpers outside the production path. Dedup/spill and manifest-draft storage receive focused tickets. Lab telemetry measures queue wait, service time, bytes, fusion, spill, reorder depth, and destination overlap outside package identity.
