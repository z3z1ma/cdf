Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Unified runtime memory ledger and byte permits

## Context

VISION §6.2-6.3 requires byte-bounded backpressure and one memory story. DataFusion already supplies the allocation-accounting contract, while CDF must extend it across non-DataFusion buffers and async stages. Separate channel capacities, decoder limits, and destination budgets would overcommit process memory and create deadlocks under parallel load.

## Decision

Create a lightweight `cdf-memory` contract crate with opaque RAII leases, accounted Arrow/byte envelopes, admission requests, consumer/class telemetry, and an object-safe coordinator interface. It depends on no DataFusion, Tokio, project, source, or destination implementation. `cdf-runtime`, engine, sources, package, and destinations consume this neutral contract, so adding a destination does not inherit DataFusion's build graph.

CDF's default executor implementation adapts `cdf-memory` to the same `Arc<dyn datafusion_execution::memory_pool::MemoryPool>` installed in every DataFusion session for the run. The DataFusion pool remains byte authority; the adapter adds async availability notification, typed observations, sub-caps, and admission without maintaining a competing total. A deterministic test implementation and external embedder contract are required.

The user-facing memory budget is a process RSS ceiling. Resolution produces and records:

- requested/default process budget;
- effective host/container memory authority;
- versioned native/runtime headroom policy and bytes;
- managed pool bytes;
- spill-disk budget/path;
- tagged sub-caps such as discovery metadata.

The exact initial headroom values are calibrated from WS-L baseline evidence before A2 implementation. An explicit unsafe budget is rejected with remediation; a default may resolve downward against effective container memory and reports that choice. RSS stress independently falsifies ledger completeness.

Every data-bearing async boundary transports an accounted envelope. Shared Arrow-buffer clones share the same RAII lease; newly allocated buffers/scratch reserve capacity before allocation and reconcile to observed size before publication. Once migrated, naked `RecordBatch`/`Vec<u8>` values MUST NOT cross runtime stage, adapter, package, or destination queue boundaries.

For legacy/custom `ResourceStream` implementations, the executor reserves the descriptor's declared maximum poll/decode working set before polling, then attaches and reconciles the payload at the source boundary. CDF-owned optimized decoders MAY consume the neutral reservation handle for finer-grained accounting. Source capability conformance compares declarations with observed retained memory.

Each operator declares a minimum working set and whether state is flushable, backpressure-safe, spillable, or fixed. Admission reserves the minimum before scheduling. A task MUST NOT await additional memory while holding leases if it cannot make progress and release them without that allocation; it must flush, spill/release/retry, or fail. This is the deadlock-prevention invariant.

Budget exhaustion is enacted by the owning operator in the fixed order: flush completed output, allow downstream/backpressure to release memory, spill spillable state to budgeted durable scratch, then return a clean `Data` error with largest consumers and remediation. The coordinator observes and wakes; it does not guess how arbitrary operator state spills.

Discovery metadata keeps the accepted default 64 MiB per file, 128 MiB concurrent total, and 8 probes. The 128 MiB is a borrowing sub-cap inside the global pool, not permanently reserved memory and not total discovery size. Weighted byte permits enforce both the global pool and discovery cap before concurrent probes start.

Spilled payloads use typed references with content identity, schema, counts, and ownership/lifecycle. Spill bytes are disk-budgeted and do not release their small in-memory metadata reservation. Cleanup is idempotent and crash-aware; spill is not package or destination evidence.

## Alternatives considered

- Separate semaphore per stage: rejected because simultaneous maxima overcommit the process and shared payloads are double-accounted inconsistently.
- DataFusion pool only with synchronous failures: rejected because it lacks async pipeline admission/backpressure and CDF consumer telemetry, though it remains byte authority.
- Count every Arrow clone independently: rejected as needlessly conservative for shared buffers; shared leases preserve one ownership charge.
- Use RSS polling as allocation control: rejected because it reacts after allocation and cannot safely arbitrate concurrent operators.
- Increase the discovery metadata cap preemptively: rejected because it is a concurrent working-set cap, already configurable, and weighted permits adapt concurrency to actual requests.

## Consequences

Runtime/source/destination APIs gain neutral accounted envelopes and cannot casually clone unbudgeted payloads. Parallelism becomes memory-admitted rather than `--jobs` alone. DataFusion and non-DataFusion operators compete fairly under one finite default pool without exporting DataFusion types into extension crates. `cdf doctor`/`--explain-memory` and P1 progress consume coordinator snapshots without becoming authority.
