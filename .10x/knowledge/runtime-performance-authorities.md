Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Runtime performance authorities

## Purpose

CDF's throughput depends less on one optimized kernel than on making concurrency, memory,
backpressure, batching, spill, and deterministic assembly compose without one stage accidentally
governing the entire graph. This record is the canonical operational model.

## One authority per resource

The run has:

- one admitted CPU/work authority;
- one managed memory ledger;
- explicit disk/spill authority;
- source transport/session authority;
- per-stage bounded queues and capabilities.

Each stage may advertise local capacity or demand. It may not create a second global authority.

Examples:

- A Parquet destination with two retained objects means two objects may be retained at that stage.
  It does not mean the source may decode only two partitions.
- DuckDB scan/sink concurrency is resolved separately from DuckDB's global thread count.
- A row-group decoder may run several tasks, but nested calls must not reacquire a run permit in a
  way that serializes or multiplies the same logical work unit.
- Blocking durability/publication work uses the blocking/I/O lane; CPU-heavy encode uses CPU
  authority.

## Stage-local pressure, global throughput

The pipeline is:

```text
source inventory/task frontier
→ transfer/read
→ decode
→ validate/normalize
→ canonical segment encode/persist/hash
→ destination ingress
→ destination durability/publication
```

Each edge is byte-bounded and backpressured. When a downstream stage is full, upstream producers
eventually block at that edge. The queue's depth is not fed backward into the scheduler as a
run-wide job ceiling.

The former Parquet failure mode was:

```text
destination max_in_flight_segments = 2
→ effective_jobs = 2
→ source/decode/package graph idles most CPUs
```

The correct behavior preserves broad upstream concurrency while bounding only retained
destination demand. Object groups encode independently and results assemble by deterministic
ordinal rather than completion order.

## CPU authority

Automatic jobs resolve from:

- cgroup/host CPU slots;
- operator `--jobs`/configuration;
- source and destination capability maxima;
- compiled task/partition cardinality;
- memory demand per admitted unit;
- measured stage pressure where an adaptive controller is explicitly part of the plan.

An explicit operator value remains authoritative unless it violates a correctness/resource
minimum. Defaults should use the host without hard-coding one particular machine's thread count.

Thread count is not concurrency identity. A library can have 16 global threads while a particular
scan should use fewer workers because its 2,000-column schema requires much more memory per worker.
Conversely an ordinary narrow schema should retain full concurrency.

## Memory authority

The memory ledger tracks owned in-flight buffers and destination/source demand. Every queue,
decoder, decompressor, segment encoder, retained destination object, and foreign boundary must
either:

- hold a ledger lease for resident bytes; or
- be a measured external-library allocation represented by a declared demand/reservation model.

Transient “currently free bytes” is not a stable planning input. It fluctuates as concurrent stages
acquire/release memory and can collapse deterministic concurrency. Plan from the admitted budget,
compiled workload estimates, and per-unit demand; use leases for runtime ownership.

Escalation order:

```text
flush
→ backpressure
→ bounded spill
→ reduce typed/adaptive concurrency
→ clean resource failure
```

Never OOM, silently exceed the configured budget, or fully materialize a package as an
“optimization.”

Typed destination OOM may retry a finalized package at progressively lower destination
concurrency when:

- the failed attempt rolled back completely;
- idempotency/receipt/checkpoint invariants remain intact;
- the retry reason and selected concurrency are recorded;
- explicit operator values remain authoritative.

## Disk and spool authority

Disk is a first-class budget, not “free memory.” Finite remote seekable objects may use accounted
spools; unbounded row streams may not. Package/staging/spill artifacts need:

- exact ownership and attempt identity;
- reservation before materialization where size is known;
- bounded cleanup;
- publication atomicity;
- generic staging lease liveness across processes;
- no destination-specific heartbeat thread.

One-TiB logical processing can succeed with a much smaller physical footprint only when the
fixture compresses strongly. Do not infer that a one-TiB incompressible object fits a 250-GiB
volume.

## Batches and canonical segments

Tiny fixed batches multiply:

- channel hops;
- statistics and event overhead;
- hashing setup;
- file descriptors and metadata;
- destination scan objects;
- allocator pressure.

Current microbatching adapts within plan-recorded bounds. Canonical segmentation is an independent
plan-recorded package contract chosen for broad downstream efficiency and memory balance, not
tuned around one DuckDB scanner experiment.

Knobs must exist for values whose optimum varies materially by host/workload. Defaults should be
large enough to amortize fixed cost, byte-bounded, and resolved from observed row width. Batch or
segment size cannot be enforced by rejecting one oversized Arrow batch that the source cannot
split safely; codecs must split or represent exact slice-position authority where required.

Determinism law:

- adaptive decisions that influence identity are resolved/recorded in the plan;
- partition/task→segment/object assignment is independent of scheduling;
- `--jobs 1` and `--jobs N` produce identical identity-bearing results;
- asynchronous completion is reordered only by preassigned ordinal.

## Streaming commit and finite packages

Canonical segments become durable one at a time and may feed staged destination ingress. The
destination may acknowledge durable segments before package finalization, but no receipt is issued
until the finalized manifest identity is verified. The checkpoint commits only after verified
receipt.

Unbounded sources still rotate finite packages:

```text
stream forever
→ bounded batches
→ finite package epoch
→ final package/receipt/checkpoint
→ release retained state
→ next package ordinal resets
```

This retains constant memory and finite evidence without inventing a separate streaming calculus.

## Provenance in the canonical segment

`_cdf_package_row_ord` is destination-neutral and materialized after filtering, deduplication, and
quarantine, before segment persistence. It provides stable row order within the finite package.

Destinations that need a transactionally allocated global key compute:

```text
_cdf_row_key = allocated_range_start + _cdf_package_row_ord
```

Segment/package metadata maps ranges to package hash and segment identity. Physical Parquet object
offsets and SQL mirror tables expose the same logical provenance. Do not add destination-specific
enumeration or assume file completion order.

## File descriptors and retained objects

A pipeline can satisfy the memory ledger and still fail with `Too many open files`. Avoid:

- one simultaneously open IPC reader per segment;
- retaining all completed destination writers until finalization;
- opening directories/files again to compute counters or hashes;
- task fan-out without a descriptor/handle lifecycle.

Hash while writing. Close segment/read handles promptly. Bound concurrently active groups while
allowing queued work to remain as lightweight authorities/paths, not live handles.

## Observability

Hot-path rendering is rate-limited and consumes events asynchronously. Every performance
investigation should be able to explain:

- admitted and effective jobs;
- per-stage active/queued capacity;
- memory budget, managed peak, external estimate, and spill;
- batch/segment/object sizes;
- source physical bytes/requests/retries/cache/spool;
- destination scan/sink concurrency and retry;
- phase wall/CPU/rows/bytes;
- receipt/checkpoint completion.

Telemetry that changes scheduling must be part of an explicit deterministic controller or affect
only non-identity-bearing pressure. Do not let unrecorded timing race change segment identities.

## Review checklist

For every runtime performance change:

1. Does a local capacity accidentally cap the whole run?
2. Is one CPU/memory/disk authority still singular?
3. Does any new buffer live outside accounting?
4. Can an unbounded input grow memory or disk without a checkpoint/eviction bound?
5. Are explicit knobs authoritative and defaults adaptive rather than magical constants?
6. Is identity invariant across jobs/scheduling?
7. Does actual I/O telemetry remain distinct from planning estimates?
8. Can cancellation/failure release all handles, leases, spools, and staging state?
9. Has the relevant end-to-end cell preserved or improved performance?
10. Was the superseded slow path deleted?
