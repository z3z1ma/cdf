Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# P3 C4 local FineWeb scaling

## Observation

On the named Apple M5 Pro host, a four-partition FineWeb Parquet run scales from 52.86 seconds at jobs=1 to 43.32 seconds at jobs=2 and 40.67 seconds at jobs=4. Jobs=4 is the observed knee: package execution completes in 7.329 seconds, while the finalized-package Parquet destination consumes 33.069 seconds, so additional upstream partition concurrency cannot materially reduce end-to-end wall time without changing the destination ingress boundary.

Peak RSS remains below the configured 4 GiB run budget: 664,600,576 bytes at jobs=1, 999,538,688 bytes at jobs=2, and 1,534,377,984 bytes at jobs=4.

## Procedure

The source object was the existing 2,147,509,487-byte FineWeb fixture at `/Users/alexanderbut/code_projects/tmp/cdf-perf/fineweb-000_00000.parquet`. Four hard links with distinct planned paths created four real file partitions without duplicating source storage. The fixed input was therefore 8,590,037,948 logical source bytes. `cdf schema pin` observed all four Parquet footers using format metadata before the timed runs.

The release CLI was built from commit `2f43cb0d` with `CARGO_BUILD_JOBS=12 cargo build --release -p cdf-cli --locked`. Each cell removed only the prior state database, package directory, and Parquet destination, retained the same pinned schema and source files, then ran:

```text
/usr/bin/time -lp cdf run fineweb.documents --jobs N --quiet --progress never --color never
```

The destination was `parquet://.cdf/destination`. Every cell processed 4.2 million rows into 460 canonical segments, verified a destination receipt, and committed the checkpoint. Observations:

| Jobs | Wall | User | System | Peak RSS | Speedup | Input rate |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 52.86 s | 58.41 s | 49.14 s | 664,600,576 B | 1.000x | 162.5 MB/s |
| 2 | 43.32 s | 58.66 s | 38.15 s | 999,538,688 B | 1.220x | 198.3 MB/s |
| 4 | 40.67 s | 59.97 s | 28.11 s | 1,534,377,984 B | 1.300x | 211.2 MB/s |

The jobs=4 run wrote 8,821,479,960 package bytes and 14,371,954,410 destination bytes. Including source input, it moved 31,783,472,318 bytes through the local data path, about 781.5 MB/s aggregate. Its durable phase telemetry recorded:

| Phase | Duration | Operations |
|---|---:|---:|
| source_read | 7.873 s | 4,248 |
| decode | 1.892 s | 4,244 |
| validation_normalization | 0.283 s | 8,472 |
| package_execution | 7.329 s | 460 |
| destination_write_receipt | 33.069 s | 460 |
| checkpoint_gate | 0.005 s | 1 |

`segment_encode` and `persist_hash` operation durations are additive concurrent work telemetry rather than wall-clock stages; they were 48.586 and 16.648 seconds respectively.

## What it supports or challenges

- It supports production partition fan-out: jobs=2 and jobs=4 both improve the same large governed run, and the jobs=4 package phase is only 18% of wall time.
- It names the current roofline for this cell: finalized-package Parquet destination work is 81% of jobs=4 wall time. The source scheduler is no longer the limiting stage at the knee.
- It supports bounded memory for this 8 GiB input and jobs range, but does not by itself prove the 100 GiB/1 TiB constant-memory law.
- The permanent fixed-package-id matrix in `.10x/tickets/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md` proves package/segment/receipt/state identity across jobs. Timed CLI package hashes intentionally differ because normal CLI package ids include run identity.

## Limits

This is one warm local APFS host, one repeated-content FineWeb object, four file partitions, and one finalized-package filesystem Parquet destination. Hard links mean source pages can share the OS cache; the 23.2 GB of package and destination output is real. The result does not claim network, PostgreSQL, DuckDB, cold-cache, or more-than-four-partition scaling. A single sample per jobs value locates the large-workload knee but is not a variance-aware regression baseline.
