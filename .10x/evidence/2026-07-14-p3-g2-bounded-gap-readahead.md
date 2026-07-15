Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# P3 G2 bounded gap readahead

## Observation

Exact range batches now distinguish returned logical bytes, useful physical coverage, physical transfer bytes, request count, and prefetch waste. This matters when logical ranges overlap: summing returned slices can exceed physical bytes and previously made `physical - logical` an invalid waste measure.

Calling the batch range API is the codec's explicit assertion that bounded extra physical bytes are harmless; only exact requested slices cross back to the codec. The source/controller owns the physical policy. Local sources remain conservative (overlap and adjacency only). HTTP and object-store sources use one fixed initial remote policy: gaps at most 64 KiB may coalesce only while the complete physical request remains within the source request ceiling and at most 1.25x the useful byte coverage. Policy values are runtime tuning, outside package identity. The controller reports actual physical/useful bytes and waste at EOF.

## Procedure

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime bounded_gap_readahead_obeys_amplification_and_reports_exact_waste -- --nocapture`
  - Passed. Two 8-byte ranges separated by 2 bytes became one 18-byte request: 16 logical, 16 useful, 18 physical, 2 waste, one request. Two 4-byte ranges separated by 4 bytes remained two 4-byte requests because coalescing would be 1.5x amplification.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime observed_byte_source -- --nocapture`
  - Passed. Overlapping ranges report 18 returned logical bytes, 16 useful bytes, 16 physical bytes, zero waste, and two reused bytes across the combined sequential/range observation.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-transport-http http_range_batch_coalesces_requests_and_preserves_logical_order -- --nocapture`
  - Passed. The real loopback HTTP provider retained exact logical ordering, generation conditions, two physical requests under the amplification ceiling, and zero memory after release.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime -p cdf-transport-http -p cdf-source-files -p cdf-format-parquet`
  - Passed: runtime 48 plus one ignored benchmark; HTTP transport 7; source-files 46; Parquet 3; build-graph and doc tests passed.
- Strict Clippy across runtime, HTTP transport, source-files, Parquet, engine, project, and CLI passed with allowances only for the known inherited workspace lint set.

## What this supports or challenges

This reduces high-BDP request latency for selective or disk-over-budget Parquet without making full spooling mandatory and without permitting unbounded egress or memory. It preserves exact logical payload/order and gives later BDP feedback a truthful objective: request reduction versus measured physical amplification.

## Limits

The 64 KiB/1.25x policy is a fixed conservative starting mode, not the final adaptive controller. Per-origin RTT/throughput feedback, throttle reduction, manual/fixed mode rendering, retry budgets, and cancellation chaos remain open. The policy does not affect growing-spool reads, whose ordinary full-scan path is already one sequential request.
