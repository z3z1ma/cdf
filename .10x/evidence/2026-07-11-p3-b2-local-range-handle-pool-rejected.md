Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-b2-parquet-codec.md

# Local range handle pool experiment rejected

## What was observed

An eight-handle local `ByteSource` pool was tested to determine whether repeated file open/close calls caused the native Parquet CPU regression. Independent handles were required: cloned Unix file descriptors share one open-file-description offset and concurrent seek/read initially produced an early EOF, which the 150,000-row native streaming test caught.

After correcting the pool to open independent handles, all 15 file-source tests and strict Clippy passed. Three fresh release TLC-to-DuckDB runs measured wall/CPU seconds of 2.41/1.83, 1.52/1.96, and 1.83/1.80. Median wall was 1.83 seconds and median CPU 1.83 seconds, no improvement over the unpooled native path's 1.63/1.80 medians.

The experiment was fully reverted. No pool, semaphore, Tokio sync feature, or dead code remains.

## Procedure

- Added an eight-permit pool of independently opened Tokio files behind the neutral local range provider.
- Ran `cargo test -p cdf-source-files --lib` and strict Clippy.
- Rebuilt the release CLI.
- Ran three fresh projects over `/private/tmp/yellow_tripdata_2024-01.parquet` into fresh DuckDB targets with `/usr/bin/time -p`.
- Reverted the entire experiment after the measured no-gain result.

## What this supports or challenges

Repeated open/close syscalls are not the dominant local native-path tax. Subsequent work should measure copied bytes, Arrow/Parquet range request cardinality, allocator cost, and page-cache behavior before considering mmap/pread. The unsafe mmap gate remains closed pending that evidence.

## Limits

Three wall samples are noisy, but CPU also failed to improve. This is sufficient to reject the extra pool complexity, not to identify the dominant cost.
