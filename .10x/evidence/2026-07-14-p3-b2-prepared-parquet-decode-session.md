Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# P3 B2 prepared Parquet decode session

## Observation

The release FineWeb local path improved from 9.13 to 7.46 seconds end to end after replacing stateless per-unit format decoding with a prepared per-file decode session. Package execution fell from 8.723 to 6.201 seconds and the cumulative decode metric fell from 6.163 to 3.556 seconds. The raw arrow-rs reference for the same 2,147,509,487-byte file was 3.204 seconds, so CDF's decode stage moved from about 0.52x to about 0.90x the raw decoder while preserving 1,058,640 rows, 115 deterministic segments, package finalization, DuckDB receipt verification, and checkpoint commit.

The defect was repeated immutable metadata work: one planning pass produced 1,059 row-group units, then every stateless decode call built a new Parquet stream reader and reloaded/reparsed the same 3.9 MiB footer. The neutral `FormatDriver` boundary now prepares one `FormatDecodeSession` per source generation. Parquet retains arrow-rs `ArrowReaderMetadata` in that session and creates unit readers with `new_with_metadata`; Arrow IPC similarly retains its parsed footer. CSV, NDJSON, and JSON use the same session contract without source-runtime format branches.

## Procedure

1. Downloaded the exact public FineWeb object used by the live smoke test to `/Users/alexanderbut/code_projects/tmp/cdf-perf/fineweb-000_00000.parquet`.
2. Built `cdf-p3-lab` in release mode and ran its raw `ArrowParquet` reference at 65,536 rows per batch: 3.203892458 seconds, 1,058,640 rows, 5,165,983,332 logical Arrow bytes.
3. Built the CDF CLI in release mode and ran the local file through the ordinary project plan/package/DuckDB/gate path. Before: 9.13 seconds wall; after: 7.46 seconds wall.
4. Read `phase_measured` ledger events for both runs. Before/after nanoseconds: decode 6,162,706,791 / 3,556,202,339; package execution 8,722,783,833 / 6,201,255,666; segment encode 6,942,691,414 / 6,963,474,502; persist/hash 1,815,875,293 / 1,546,048,835.
5. Added an eight-row-group conformance fixture whose byte source counts suffix reads. The count is fixed after session preparation and remains unchanged while all eight units decode.
6. Ran the focused runtime/format/file-source suite (99 passing, two performance tests ignored), strict no-dependency clippy for the six affected crates, and a workspace all-target check.

## What it supports or challenges

- Supports a prepared decode session as the neutral extension boundary for reusable per-file codec state.
- Supports the claim that the first large FineWeb local gap was redundant footer work, not Parquet decoding or DuckDB finalization.
- Challenges any design that makes deterministic decode units stateless when their codec shares immutable file metadata.
- Leaves segment encoding, row-group execution parallelism, growing-spool overlap, and peak RSS as measured open work; this is not B2 or G2 closure evidence.

## Limits

The before/after local runs used a warm local file and the same host, but different package and destination generations. The phase metrics overlap and their cumulative durations must not be summed as wall time. The workspace all-target check reached unrelated pre-existing `cdf-dest-postgres` live-test type errors involving `OpenedPartitionStream`; affected crates and the production CLI compiled successfully. The remote release path has not yet been repeated after this change.
