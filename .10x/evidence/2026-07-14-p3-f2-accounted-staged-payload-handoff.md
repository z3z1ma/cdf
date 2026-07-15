Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# Accounted staged-payload ownership handoff

## Observation

The package-to-staged-destination edge was reserving the same Arrow buffers twice: once while the engine retained segment batches for encoding and again after the batches were cloned into the staged-ingress queue. This artificial collision limited segment encoding to four workers and failed under wider concurrency even though the physical allocations had a single lifetime.

The replacement moves the batches and their existing `MemoryLease` values together in one `DurableSegmentPayload` after the segment is durably published. The staged destination owns that payload until synchronous consumption returns or the background work item is dropped. There is no destination-specific orchestration branch and no compatibility path retaining the duplicate reservation.

## Procedure

Three local FineWeb Parquet-to-DuckDB runs used the same 2.147 GB input and resource configuration:

1. Removing the arbitrary four-worker cap without changing ownership failed after 1.47 seconds at 1,133,953,024 bytes maximum RSS: `canonical segment requires 145950048 bytes ... shared memory budget exhausted`.
2. Draining completed encode work under reservation pressure, while retaining the duplicate staged reservation, failed after 1.51 seconds at 1,311,440,896 bytes maximum RSS: `staged ingress requires 33348051 queue bytes ... budget exhausted`.
3. Moving the existing leases into staged ingress completed in 5.35 seconds at 2,021,523,456 bytes maximum RSS. It produced 1,058,640 rows and 115 segments, then verified the destination receipt and committed the checkpoint.

The successful run reported these phase measurements:

- decode: 528,498,390 ns
- validation: 71,572,477 ns
- segment encode, cumulative worker time: 11,055,865,750 ns
- segment persist and hash, cumulative worker time: 3,774,716,581 ns
- package finalize: 23,957,417 ns
- package execution wall: 4,168,348,333 ns
- destination finalize: 210,138,959 ns
- checkpoint: 532,792 ns

The same fixture before this change spent 5,008,483,958 ns in package execution. The owned handoff reduced that critical phase by 16.8%; whole-run wall stayed approximately flat at 5.35 seconds versus 5.37 seconds because preparation and other work outside the package phase now dominate.

Verification:

```text
CARGO_BUILD_JOBS=12 cargo test -p cdf-engine -p cdf-project --lib --locked -j12
cdf-engine: 125 passed, 0 failed, 6 ignored
cdf-project: 168 passed, 9 failed, 9 ignored
```

All nine project failures were pre-existing and outside this ownership edge. Focused staged-ingress, staged abort/failure, and durable-payload lifetime tests passed. The payload lifetime test retains a payload beyond the callback, observes live managed bytes, drops it, and observes managed bytes return to zero.

## What it supports or challenges

- Supports one physical allocation having one moving memory owner across engine and destination capabilities.
- Supports removing arbitrary concurrency caps after fixing the resource model they concealed.
- Supports the staged-ingress extension boundary: generic orchestration branches on ingress capability, not destination identity.
- Challenges the assumption that a second reservation over cloned Arrow references is conservative. It is double accounting and can manufacture deadlock-like budget failures.

## Limits

The process maximum RSS increased about 6.7% from the prior local DuckDB run because more encode work may now proceed concurrently; the run remains bounded by the configured worker/memory calculation and DuckDB's native envelope. This observation does not close the global allocation-owner matrix, geometric stress law, package write-roofline target, or remaining preparation overhead.
