Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-d4-parquet-streaming-writer.md

# Streaming Parquet destination closeout

## What was observed

The current 128 MiB high-entropy release control wrote 134,219,889 physical bytes at 1,006.8 MiB/s versus 1,569.3 MiB/s for equal-byte raw durable writes, a 0.642 roofline ratio. The permanent benchmark threshold is 0.60.

The complete destination suite passed 27 non-ignored tests and strict all-target Clippy. Coverage includes filesystem and object-store append/replace, duplicate replay, abort, orphan reuse, tamper/missing object detection, manifest/pointer identity, correction sidecars, schema/type rejection, zero-data behavior, and session/wrapper receipt equivalence.

The session-equivalence helper previously materialized all verified segments. It now consumes `verified_commit_segment_stream` with the destination's shared memory coordinator and one-segment maximum, matching production boundedness.

## Procedure

```text
cargo test -p cdf-dest-parquet --locked
cargo clippy -p cdf-dest-parquet --all-targets --locked -- -D warnings
cargo test -p cdf-dest-parquet --release local_streaming_parquet_reaches_sixty_percent_of_write_roofline --locked -- --ignored --nocapture
```

## What this supports or challenges

This supports bounded row-group encoding, local atomic installation, bounded multipart remote/custom upload, semantic correctness, and the ≥60% local write-roofline target. It challenges the assumption that test-only whole-package readers are harmless; leased materialization can invalidate the same memory contract production enforces.

## Limits

The remote multipart path is verified with deterministic object-store tests and byte admission, not a WAN throughput profile. G/D5 own environment-wide remote overlap measurements; D4 establishes the adapter implementation and local roofline.
