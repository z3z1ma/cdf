Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Runtime memory accounting and backpressure audit

## Question

What should “one memory ledger” mean across Arrow/DataFusion, decoders, channels, package/destination staging, metadata discovery, and spill, and does current source enforce the existing discovery budget?

## Sources and methods

Read the runtime architecture/performance specs, current DataFusion pinned `MemoryPool`/consumer/reservation implementations, CDF batch payload/header types, engine execution, discovery budget/scheduler, exact-row dedup policy, and P3 memory/parallelism tickets. Searched runtime crates for pool, reservation, RSS, spill, and batch accounting use.

## Findings

CDF does not currently construct or share a bounded DataFusion `MemoryPool`; DataFusion's default is unbounded. No general runtime reservation or spill authority exists. `BatchHeader.byte_count` uses Arrow `get_array_memory_size`, but it is descriptive and does not reserve capacity.

DataFusion provides the correct accounting substrate: named `MemoryConsumer`, RAII `MemoryReservation`, finite greedy/fair-spill pools, and consumer tracking. Its `try_grow` is synchronous and reports insufficient capacity; operators remain responsible for deciding whether to flush, wait, spill, or fail. Therefore byte-bounded async backpressure cannot be implemented by a message-count channel or the pool alone.

Arrow batches are cheaply cloneable through shared buffers. Counting every clone as new bytes is safe but can destroy utilization; counting no clones leaks accounting. A shared lease must follow shared buffers across channel ownership, while allocating transforms reserve output/scratch before publication. Pipeline stage APIs must not accept naked `RecordBatch` values once the ledger lands.

Waiting for memory while holding input/scratch can deadlock a full pipeline if every worker does the same. Operators need a declared minimum working set and admission must reserve it before work starts; growth while holding memory is legal only if the task can still make progress/release memory without further allocation. Otherwise it must flush/spill/release and retry or fail.

The existing discovery default—64 MiB per file, 128 MiB total in-flight, 8 probes—is a sensible concurrent sub-budget, not a limit on dataset/file-count scale. Typical ranged footers are far smaller; byte permits allow eight small probes while limiting pathological maximum probes to two. The cap is configurable and recorded.

Current discovery source does not implement the advertised scheduler: it iterates selected candidates sequentially. `max_metadata_bytes_per_file` bounds each probe, while `max_total_in_flight_bytes` and `max_concurrent_probes` are only validated/serialized and never consumed. Sequential execution stays under total bytes but does not deliver the claimed concurrency. A future naive parallel loop would violate the cap unless weighted permits land first.

Package-wide exact-row/keyed dedup intentionally retains package-order identity state and currently retains accepted batches. It is the clearest required spill consumer; exempting it would make the constant-memory guarantee false.

## Conclusion

A lightweight `cdf-memory` contract crate should define opaque leases, accounted envelopes, admission, and telemetry without depending on DataFusion, Tokio, product, or destination crates. CDF's default executor implementation adapts that contract to the exact shared DataFusion pool used by query execution; test and future embedding implementations prove the contract is not DataFusion-type leakage. `cdf-runtime`, sources, engine, package, and destinations consume the neutral handle.

Process RSS budget, managed-data budget, and spill-disk budget are distinct. The user-facing memory budget is an RSS ceiling; a versioned resolved headroom policy subtracts runtime/allocator/native-library allowance before creating the managed pool. Exact initial headroom must be calibrated by the pre-optimization lab and recorded, not guessed in code. Explicit budgets that cannot fit one legal operator working set fail before execution.

Discovery's 128 MiB default should remain, as the user accepted, but become a tagged sub-cap borrowing from—not reserving separately from—the global pool. A weighted byte permit plus eight-task cap governs concurrent probes. All sub-budget use appears in memory explanation/telemetry.

Legacy/custom `ResourceStream` implementations need not learn DataFusion. Runtime admission reserves the source's declared maximum poll/decode working set before polling and attaches/reconciles the returned payload lease at the source boundary. CDF-owned high-throughput decoders may use the neutral granular reservation handle internally. Capability conformance falsifies declarations that understate working memory.

## Limits

Arrow memory-size methods are estimates and native libraries allocate outside the pool; RSS stress remains the independent enforcement backstop. A2 must test shared-buffer clones, transform expansion, cancellation/drop, panic/error cleanup, and deadlock scenarios before claiming authority.
