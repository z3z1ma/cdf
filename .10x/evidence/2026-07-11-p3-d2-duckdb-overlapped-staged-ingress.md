Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-d2-duckdb-arrow-bulk.md, .10x/specs/streaming-destination-ingress.md, .10x/decisions/compact-lossless-destination-row-provenance.md

# DuckDB overlapped staged-ingress evidence

## Observation

The release CLI loaded the local January 2024 NYC TLC Yellow Taxi Parquet fixture: 2,964,624 rows, 19 source fields, 16 canonical segments, and 104 MiB of encoded package data. The run completed with a verified DuckDB receipt and committed checkpoint in 1.96s real, 1.98s user, and 0.19s system time. The immediately preceding equivalent LZ4 run before staged ingress took 2.52s; its destination phase took 1.258s. With staged ingress, final package binding and receipt took 0.252s. This is a 22% end-to-end reduction and an 80% reduction in post-package destination wall time.

## Procedure

The fixture was `/private/tmp/yellow_tripdata_2024-01.parquet` on an Apple M5 Pro. A fresh local CDF project used one local Parquet resource, append disposition, DuckDB destination, release `cdf`, and `/usr/bin/time -p`. Package trace and receipt artifacts were verified after the run. Focused verification passed 28 active `cdf-runtime` tests, 24 active `cdf-dest-duckdb` tests, and the staged live/replay/merge/duplicate/failure cases in `cdf-project`.

The staging-pressure control then compared fresh projects with identical input under one-segment/64 MiB and two-segment/128 MiB declared bounds. Three old-bound runs measured 2.71s, 1.83s, and 2.15s (median 2.15s). Three new-bound runs measured 2.51s, 1.73s, and 1.89s (median 1.89s), a 12.1% median wall reduction. CPU time and committed outputs remained equivalent. The retained bound permits one segment in the writer plus one queued segment while remaining under the shared memory ledger.

## What this supports

The old finalized-package path reread and decompressed every IPC segment after package completion. The new path sends each hash-complete durable in-memory Arrow segment through the destination-neutral staged-ingress session on the declared `duckdb.connection` lane. DuckDB owns only its native transaction, vectorized persistence, and compact physical provenance mapping. Generic orchestration owns attempt authority, byte admission, exact final binding, receipt recording, and the checkpoint gate without destination-name dispatch.

## Limits

The 1.96s run is warm local-I/O evidence, not the full HTTPS roofline. Because destination append overlaps package persistence, the package interval includes useful destination work; the 0.252s figure is final binding/receipt only and is not total isolated DuckDB CPU time. Full jobs, crash, and envelope closure remain owned by active P3 tickets.
