Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-g2-range-readahead-spool-controller.md, .10x/tickets/2026-07-11-p3-b2-parquet-codec.md, .10x/tickets/done/2026-07-11-p3-a3-file-manifest-slice-authority-regression.md

# HTTP Parquet sequential spool and positioned slicing

## What was observed

The execution path for a full or unknown-coverage remote Parquet scan now uses one sequential, generation-preconditioned transfer into a bounded temporary spool. Parquet footer discovery remains range-bounded. The obsolete execution exports that decoded Parquet through an unconditional `RangeChunkReader` were removed.

The canonical assembler now recognizes a `FileManifest` as invariant under decoded-batch slicing. A wide or oversized file batch can therefore be split at canonical row/byte boundaries while every segment retains the exact terminal file identity. Cursor positions remain unsplittable without source-provided slice authority.

## Procedure

- `cargo test -p cdf-project http_parquet_auto_pin_plan_preview_and_run_use_file_runtime -- --nocapture` passed. The fixture asserts bounded ranged GETs for discovery and exactly one un-ranged `If-Match` GET for each of preview and run.
- `cargo test -p cdf-declarative inferred_https_parquet_confirmation_uses_only_bounded_ranges -- --nocapture` passed.
- `cargo test -p cdf-source-files` passed all 14 tests, including HTTP/object-store/local transport coverage.
- `cargo test -p cdf-engine segmentation::tests -- --nocapture` passed 14 active tests. The new regression test proves a five-row positioned file batch splits into 4+1 rows with the identical manifest on both segments, while the cursor rejection test remains green.
- A fresh public live run executed `cdf add tlc.yellow https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet` followed by `cdf run tlc.yellow`. It loaded 2,964,624 rows in 46 segments into DuckDB and committed the verified checkpoint. The unoptimized debug run completed in 43.85 seconds wall time; extraction through package finalization took approximately 12.3 seconds from event timestamps. The prior pathological many-round-trip execution was not observed.

## What this supports

This supports the immediate full-scan policy correction: bounded footer ranges are retained, while execution no longer serializes Parquet page/chunk range requests. It also supports raising the native read batch default from 1,024 to 65,536 rows without weakening exact checkpoint position semantics for file partitions.

## Limits

This does not close B2 or G2. `FormatRead` still collects decoded batches before the source stream publishes them, selective row-group range planning is not implemented, and the live measurement is a debug end-to-end observation rather than the release roofline benchmark.
