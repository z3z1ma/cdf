Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a12-byte-first-segments-shared-arrow-accounting.md, .10x/tickets/2026-07-11-p3-b2-parquet-codec.md

# Parquet stream, byte-first segments, and retained-memory evidence

## What was observed

The public January 2024 TLC Parquet object contains 2,964,624 rows, 49,961,641 encoded source bytes, and 425,055,296 logical Arrow bytes. Raw arrow-rs Parquet decode at 65,536 rows measured 99.1–126.8 ms across five release runs. Instrumenting the new CDF codec stream separated 113.9 ms in `ParquetRecordBatchReader::next` from 0.2 ms total reconciliation/evidence envelope work, confirming that the earlier 628 ms aggregate decode phase included the sequential HTTP spool rather than hidden schema overhead.

Under canonical segmentation v1, the live HTTPS run produced 46 segments totaling 109,624,620 bytes. Phase telemetry measured 627.6 ms aggregate open/download/decode, 359.1 ms IPC encode/hash, 363.1 ms durable publish, 24.8 ms finalize, and 1,617.4 ms package execution. The row ceiling forced encoded segments averaging 2.38 MiB.

Canonical segmentation v2 produced 16 segments totaling 109,164,640 bytes. The comparable live run measured 625.5 ms aggregate open/download/decode, 339.5 ms encode/hash, 141.8 ms durable publish, 22.8 ms finalize, and 1,363.2 ms package execution. Rows, source manifest position, schema, receipt count, and checkpoint completion remained exact. This is a 30-to-16 segment reduction, 60.9% durable-publish reduction, and 15.7% package-execution reduction.

An uncompressed IPC experiment reduced encode/hash to 191 ms but expanded package data to 437,247,712 bytes. After correcting shared-buffer accounting it completed, but local end-to-end wall time was 2.77 s versus a 2.63 s median across three LZ4 runs; LZ4 remains canonical. The experiment also reproduced the old memory bug: a 28 MiB zero-copy IPC segment was reported as 667 MiB because shared backing capacity was summed per column. The new allocation-aware counter counts a shared allocation once and is permanently unit-tested.

The final generic format-stream implementation completed a fresh local release TLC run in 2.52 s wall (2.06 s user, 0.15 s system): 122.9 ms decode, 342.0 ms encode/hash, 149.3 ms durable publish, 847.2 ms package execution, and 1,257.6 ms DuckDB write/receipt across 16 segments.

## Procedure

- Ran five `cdf-p3-lab reference-worker` Arrow Parquet reference measurements against `/private/tmp/yellow_tripdata_2024-01.parquet` at 65,536 rows.
- Ran fresh release `cdf init`, `cdf add`, and `cdf run` projects against both the public HTTPS URL and local cached file, with CLI phase telemetry persisted in `cdf_run_events`.
- Compared v1 (46 segments), v2 32-MiB target (16 segments), an intentionally rejected 64-MiB target (8 segments exceeding the declared 64-MiB retained window), and uncompressed IPC.
- Verified the production path through the generic format-stream entry point; `cdf-source-files` no longer contains a Parquet execution branch.
- Ran all 86 active `cdf-engine` tests, all 49 active `cdf-package` tests, all 11 `cdf-memory` tests, all 13 `cdf-source-files` tests, focused HTTP project and streaming Parquet tests, and strict clippy across the touched runtime crates.

## What this supports or challenges

This supports sequential spool for full/unknown remote coverage, incremental Parquet batch publication, byte-first durable segmentation, format-driver ownership, allocation-aware memory authority, and retaining LZ4 on current end-to-end evidence. It challenges any claim that raw Parquet decode or schema reconciliation explains the prior wall time.

## Limits

The HTTP comparison includes public-network variance and the aggregate decode phase still combines transport spool with codec decode; G3 owns explicit download/decode overlap telemetry. Segment encoding remains sequential and LZ4 encode/hash remains about 350 ms locally. Projection/predicate pushdown, row-group parallelism, parallel segment encoding, and the final TLC envelope remain open under B2/C2/E4/G3/G4.
