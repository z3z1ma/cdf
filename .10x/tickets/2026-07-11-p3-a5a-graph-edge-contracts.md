Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p3-a5-streaming-operator-graph.md
Depends-On: .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/specs/streaming-operator-graph.md

# P3 A5a: compiled graph and accounted edge contracts

## Scope

Implement engine-neutral graph/node/capability descriptors, compile the existing planned resource transition into that graph, and implement ownership-transfer data/outcome envelopes plus byte-accounted bounded edges on the injected host. Add static gates preventing naked Arrow/byte payloads and source/format/destination-name dispatch across production graph edges.

## Acceptance criteria

- A mock external source/operator/destination graph compiles through capabilities without generic runtime branches.
- Every data edge transfers partition/local sequence, source-position/schema authority, payload, outcomes, and one shared/owned ledger reservation.
- Slow-consumer and cancellation tests prove byte backpressure, ownership release, first-failure cancellation, and no detached tasks.
- Graph identity records semantic nodes/fusion/durable boundaries while runtime queue timing/capacity observations remain nonidentity.
- Static tests reject naked `RecordBatch`/`Vec<u8>` production edge payloads and private runtimes/pools.

## Evidence expectations

Graph goldens, mock capability conformance, byte/RSS pressure traces, cancellation/panic tests, architecture scans, and edge overhead benchmark.

## Explicit exclusions

No fused business kernels, package writer migration, destination driver optimization, or parallel partition frontier.

## Blockers

None.

## References

- `.10x/decisions/compiled-fused-streaming-operator-graph.md`
- `.10x/specs/execution-host-structured-runtime.md`
- `.10x/specs/runtime-memory-backpressure.md`
- `.10x/specs/streaming-operator-graph.md`

## Progress and notes

- 2026-07-11: Added the engine-neutral semantic graph artifact and validation boundary in `cdf-runtime`: versioned node/executor/ordering/fusion/durability declarations, deterministic semantic hashing, duplicate/reference/cycle validation, and strict blocking-lane and working-set invariants. Runtime queue capacity remains deliberately outside graph identity.
- 2026-07-11: Added the first accounted ownership-transfer edge. Arrow/byte payloads can cross it only through `cdf-memory` leases; bounded outcome metadata has its own control-memory lease rather than an asserted size. Global byte pressure blocks producers independently of item capacity, shared leases release on drop/cancellation, and cancellation closes admission before enqueue. Focused runtime tests and strict Clippy pass. Graph compilation from the planned resource transition, structured first-failure scope integration, static architecture gates, and edge overhead evidence remain open.
