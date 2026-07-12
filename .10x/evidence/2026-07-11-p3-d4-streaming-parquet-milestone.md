Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-d4-parquet-streaming-writer.md

# Streaming Parquet destination milestone

## What was observed

The Parquet destination no longer converts an entire segment to `Vec<u8>`. A verified `CommitSegment` is moved onto the declared `parquet.encode` lane, preserving its package memory-retention owner. Arrow batches feed an ArrowWriter configured for canonical 64k-row groups; output streams into a spill-accounted temp file and SHA-256 is updated on each successful write. The file is synced before upload.

Upload reads fixed 8 MiB chunks under a destination memory lease, permits four concurrent multipart parts, applies asynchronous backpressure when the shared ledger is busy, aborts incomplete uploads on local read or part failure, and publishes atomically only at multipart completion. The package manifest and replace pointer remain the final visibility gate.

For a filesystem store, encoding now creates the staging file under the destination root and atomically installs it with no-clobber rename and parent-directory sync. This removes the previous temp-file reread and second full write. An orphan at the immutable key is accepted only after a bounded streaming SHA-256 comparison. The filesystem conformance test verifies the staging directory is empty after successful publication.

All 27 `cdf-dest-parquet` tests passed. They cover filesystem and in-memory object-store writes, segment/session equivalence, duplicate no-op, append/replace, current pointers, missing/tampered object verification, correction sidecars, interrupted correction publication, abort before write, unsupported/duplicate schema diagnostics, root prefixing, and zero-data behavior.

The release median-of-three local roofline uses 8,388,608 high-entropy rows (two 64-bit columns, 128 MiB logical input), hashes and syncs Parquet output, and compares it with raw durable writes of the exact physical output byte count. ArrowWriter defaults produced 167,252,874 bytes at 332.3 MiB/s versus 1,659.9 MiB/s raw (0.200x). The final bounded policy—1M rows/32 MiB per row group, 1M-row internal write batch, 64k rows/8 MiB per data page, dictionary disabled, statistics disabled—produced 134,219,889 bytes. Two final median runs measured 1,103.8 MiB/s versus 1,802.4 MiB/s raw (0.612x) and 1,028.4 MiB/s versus 1,609.8 MiB/s raw (0.639x). Dictionary/statistics are query accelerators, not fidelity requirements, and are excluded until workload evidence justifies their measured write tax.

## Procedure

```text
cargo test -p cdf-dest-parquet --all-targets
cargo test --release -p cdf-dest-parquet local_streaming_parquet_reaches_sixty_percent_of_write_roofline -- --ignored --nocapture
```

## What this supports or challenges

This supports constant-size destination payload buffering per segment/row group/multipart policy, verified input retention, hash-while-write, atomic multipart visibility, removal of the destination's full encoded-byte materialization, and the ≥60% local device-write roofline target for the measured high-entropy primitive workload.

## Limits

Encoding currently completes into the durable temp file before remote upload begins; remote encode/upload overlap is not yet proven. High-file-count stress, crash cleanup after process death, measured RSS, multipart network profiling, and nested/string-heavy roofline classes remain D4 scope.
