Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# P3 G2 transport-neutral exact-range batching

## Observation

Range concurrency and lossless overlap/adjacency coalescing now belong to the neutral `ByteSource` contract rather than the Parquet codec. A codec submits exact logical extents in its required order. The source boundary plans bounded physical requests using the provider's declared request-size and concurrency ceilings, issues them concurrently, and returns zero-copy logical slices in the original order.

Each coalesced physical response owns one shared memory lease. Logical slices retain that complete lease until the last slice drops, so overlapping or adjacent requests cannot make retained source memory disappear from the ledger. The result reports logical bytes, physical bytes, and physical request count independently.

## Procedure

The runtime controller test requests five logical extents in deliberately non-physical order, including an overlap. It observes:

```text
logical extents: 5
logical bytes: 20
physical requests: 2 (0..12, 20..24)
physical bytes: 16
retained ledger bytes before drop: 16
retained ledger bytes after drop: 0
```

The HTTP provider fixture requests three logical extents in non-physical order. Its loopback server receives two generation-preconditioned HTTP requests (`0..8` and `12..16`), while the caller receives all three exact payloads in its original order. Physical/logical bytes are both 12, request count is 2, and source memory releases from 12 to zero on drop.

The Parquet `AsyncFileReader::get_byte_ranges` implementation now delegates directly to `ByteSource::read_exact_ranges`; it contains no provider concurrency policy.

Verification:

```text
CARGO_BUILD_JOBS=12 cargo test -p cdf-memory -p cdf-runtime -p cdf-format-parquet --lib --locked -j 12
cdf-memory: 12 passed
cdf-runtime: 43 passed, 1 ignored performance benchmark
cdf-format-parquet: 3 passed

CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files -p cdf-transport-http --lib --locked -j 12
cdf-source-files: 46 passed
cdf-transport-http: 7 passed

CARGO_BUILD_JOBS=12 cargo test -p cdf-transport-http http_range_batch_coalesces_requests_and_preserves_logical_order --locked -j 12
1 passed

CARGO_BUILD_JOBS=12 cargo clippy -p cdf-memory -p cdf-runtime -p cdf-format-parquet -p cdf-source-files -p cdf-transport-http --lib --no-deps --locked -j 12 -- -D warnings
Finished successfully
```

## What this supports or challenges

This supports the extension law: format drivers express logical byte demand, while transport-neutral source capabilities control physical concurrency. Adding a new provider receives correct bounded batch behavior without a Parquet edit; adding another seekable codec can use the same contract without implementing HTTP/cloud fan-out.

It also supports generation and identity invariance. Coalescing changes request shape and memory lifetime only; exact logical bytes and order remain unchanged.

## Limits

This slice coalesces only overlapping or exactly adjacent extents, so it introduces no prefetch waste. It does not yet coalesce small gaps, adapt concurrency/range size to BDP or throttling, retry typed partial units, publish run telemetry, or prove a high-BDP throughput target. Individual logical extents larger than the source's preferred request ceiling remain one request because the current codec contract requires one contiguous logical buffer; page-streaming/splitting is separate codec work.
