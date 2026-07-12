Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-d4-parquet-streaming-writer.md

# Streaming Parquet destination milestone

## What was observed

The Parquet destination no longer converts an entire segment to `Vec<u8>`. A verified `CommitSegment` is moved onto the declared `parquet.encode` lane, preserving its package memory-retention owner. Arrow batches feed an ArrowWriter configured for canonical 64k-row groups; output streams into a spill-accounted temp file and SHA-256 is updated on each successful write. The file is synced before upload.

Upload reads fixed 8 MiB chunks under a destination memory lease, permits four concurrent multipart parts, applies asynchronous backpressure when the shared ledger is busy, aborts incomplete uploads on local read or part failure, and publishes atomically only at multipart completion. The package manifest and replace pointer remain the final visibility gate.

For a filesystem store, encoding now creates the staging file under the destination root and atomically installs it with no-clobber rename and parent-directory sync. This removes the previous temp-file reread and second full write. An orphan at the immutable key is accepted only after a bounded streaming SHA-256 comparison. The filesystem conformance test verifies the staging directory is empty after successful publication.

All 27 `cdf-dest-parquet` tests passed. They cover filesystem and in-memory object-store writes, segment/session equivalence, duplicate no-op, append/replace, current pointers, missing/tampered object verification, correction sidecars, interrupted correction publication, abort before write, unsupported/duplicate schema diagnostics, root prefixing, and zero-data behavior.

## Procedure

```text
cargo test -p cdf-dest-parquet --all-targets
```

## What this supports or challenges

This supports constant-size destination payload buffering per segment/row group/multipart policy, verified input retention, hash-while-write, atomic multipart visibility, and removal of the destination's full encoded-byte materialization.

## Limits

Encoding currently completes into the durable temp file before remote upload begins; remote encode/upload overlap is not yet proven. Local installation is single-write plus rename, but device-roofline evidence has not yet been measured. High-file-count stress, crash cleanup after process death, measured RSS, multipart network profiling, and the ≥60% write-roofline target remain D4 scope.
