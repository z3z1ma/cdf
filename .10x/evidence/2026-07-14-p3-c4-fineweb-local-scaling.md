Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# P3 C4 local FineWeb scaling

## Observation

On the named Apple M5 Pro host, a four-partition FineWeb Parquet run scales from 52.86 seconds at jobs=1 to 43.32 seconds at jobs=2 and 40.67 seconds at jobs=4. Jobs=4 is the observed full-path knee: package execution completes in 7.329 seconds, while the finalized-package Parquet destination consumes 33.069 seconds, so additional upstream partition concurrency cannot materially reduce end-to-end wall time without changing the destination ingress boundary.

The destination writer itself is not at its roofline. Its release benchmark writes Parquet at 1,716.5 MiB/s against a 2,184.7 MiB/s raw sequential-write roofline on this host, a 0.786 ratio. The 33.069-second finalized-package phase was therefore an ingress/orchestration deficit, not a device or codec ceiling; `.10x/tickets/done/2026-07-14-p3-d8-parquet-staged-parallel-ingress.md` closed that deficit with the generic staged path.

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

Two fresh same-input controls added host scheduler counters:

| Jobs | Wall | User | System | Mean CPU cores | Peak RSS | Voluntary switches | Involuntary switches | Instructions | Cycles |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | 40.67 s | 58.06 s | 38.68 s | 2.378 | 1,201,782,784 B | 12,495 | 2,350,140 | 757,662,709,846 | 405,530,147,805 |
| 4 | 41.54 s | 59.22 s | 29.00 s | 2.123 | 1,469,071,360 B | 19,441 | 1,427,699 | 746,516,897,433 | 372,247,580,608 |
| 4 | 39.39 s | 58.56 s | 27.34 s | 2.181 | 1,421,279,232 B | 19,753 | 1,407,271 | 740,193,249,139 | 361,577,435,039 |

Mean CPU cores is `(user + system) / wall`; the flat jobs=2/4 utilization and full-path wall time agree with the phase attribution. The source/package phase has stopped dominating, while finalized Parquet ingress is serial.

## Concurrency falsification and repetition

Live repetition caught two nondeterministic parked runs: one after 230 durable segments and one after 23. Every CDF CPU worker, file-source control worker, and Parquet encode worker was parked; process CPU was 0%. The generic memory reservation future had a real check/register lost-wake and shared-waker removal defect. Commits `6f7e8d3e` and `0b088671` close those interleavings with a post-registration retry, cancellation removal, and a ref-counted neutral waiter set shared by the deterministic and DataFusion coordinators.

One subsequent same-build large run again parked after 230 segments, so this evidence does not erase or over-attribute that observation. After rebuilding from the ref-counted implementation, the following completed without another park:

- Five 460-segment full finalized-Parquet repetitions, including two with diagnostic instrumentation disabled.
- Ten 460-segment full package-to-DuckDB repetitions at jobs=4, each processing the same 8.59 GB logical input in 16–19 seconds.
- Thirty-five 296-segment jobs=4 package-to-DuckDB repetitions over a generated four-partition, 517-row-group-per-file, 4.2-million-row wide-string fixture. Each completed in 5–7 seconds under a 15-second timeout; the final five used a clean release rebuild after all diagnostic-only code was removed.

The generated fixture preserves the high-cardinality scheduling shape while Zstd makes its four hard-linked source files only 5.2 MiB on disk. Its repeated decoded payload is about 8 GiB logical, so this is a scheduler/memory interleaving stressor rather than a storage-throughput benchmark. The earlier parked run remains residual risk for fresh review; closure must not claim a root cause beyond the two proven waiter defects unless another failure is reproduced with scheduler state telemetry.

## What it supports or challenges

- It supports production partition fan-out: jobs=2 and jobs=4 both improve the same large governed run, and the jobs=4 package phase is only 18% of wall time.
- It names the current scaling limit for this cell: finalized-package Parquet destination work is 81% of jobs=4 wall time. The source scheduler is no longer the limiting stage at the knee, but the destination phase remains far below the measured writer/device roofline.
- It supports bounded memory for this 8 GiB input and jobs range, but does not by itself prove the 100 GiB/1 TiB constant-memory law.
- The permanent fixed-package-id matrix in `.10x/tickets/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md` proves package/segment/receipt/state identity across jobs. Timed CLI package hashes intentionally differ because normal CLI package ids include run identity.

## Limits

This is one warm local APFS host, one repeated-content FineWeb object, four file partitions, and one finalized-package filesystem Parquet destination. Hard links mean source pages can share the OS cache; the 23.2 GB of package and destination output is real. The result does not claim network, cold-cache, or more-than-four-partition scaling. The initial jobs curve is not a variance-aware baseline; later repetitions test liveness but do not replace the lab's median-of-N throughput baselines. Exact runnable/blocked and frontier-wait durations are not yet exported as durable run telemetry; CPU time, phase attribution, memory snapshots, and OS context-switch/counter observations are the present evidence boundary.
