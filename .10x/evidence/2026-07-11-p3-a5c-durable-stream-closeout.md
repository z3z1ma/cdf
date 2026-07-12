Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a5c-durable-segment-stream.md, .10x/specs/streaming-operator-graph.md

# A5c durable segment stream closeout

## What was observed

The canonical package handoff now verifies identity and row authority before yielding, holds one memory-accounted segment window, and carries its lease through `CommitSegment` until the destination releases the batch. Project replay selects behavior solely from serialized capabilities. DuckDB, Postgres, and Parquet all declare and consume `SegmentStreaming`; each releases one segment before the next. The package Parquet archive transcodes one segment at a time.

The former DuckDB package-wide scalar row vector was deleted. Postgres' remaining conversion helper accepts one segment, not `Vec<CommitSegment>`. The generic compatibility branch reads and acknowledges one segment at a time. A permanent source architecture test scans project runtime, all first-party destination production sources, and package archive code for `read_all_segments(`, `read_commit_segments(`, and `Vec<CommitSegment>`.

## Procedure

- `cargo test -p cdf-package` — 49 passed, three explicit performance tests ignored, including the new production-materialization gate and archive determinism/tamper cases.
- `cargo test -p cdf-project generic_package_replay_and_recovery_drive_mock_runtime_without_destination_branch` — passed.
- DuckDB: 24 tests passed after streaming conversion; Postgres: 30 tests passed including live rollback; Parquet: 27 tests passed.
- Strict clippy passed for package, project, Postgres, DuckDB, and Parquet production/test targets.
- `.10x/evidence/2026-07-11-p3-a5c-verified-segment-stream-milestone.md` records one-window, tamper, undersized-window, and authority tests.
- `.10x/evidence/2026-07-11-p3-a6-100g-constant-memory.md` records 100 GiB-scale bounded spill/RSS behavior for the package-global operator.
- `.10x/evidence/2026-07-11-p3-d2-duckdb-arrow-milestone.md` records 64k-row destination batches, 1.946M TLC-shaped rows/s, and the removed scalar path's 1.868 GiB process maximum.

## What this supports or challenges

This supports all A5c acceptance criteria: durable-only handoff, bounded accounted read ownership, neutral replay/destination consumption, static exclusion of production package collection, and preservation of tamper/abort/receipt/checkpoint laws across first-party destinations.

## Limits

This does not claim concurrent overlap between extraction, transform, persistence, and destination commit. A5e owns compiled graph integration, cancellation at every cross-stage edge, lane scheduling, and end-to-end overlap profiles.
