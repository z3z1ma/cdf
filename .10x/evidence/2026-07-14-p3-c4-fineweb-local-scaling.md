Status: recorded
Created: 2026-07-14
Updated: 2026-07-15

# P3 C4 local FineWeb scaling

## Observation

On the named Apple M5 Pro host, the pre-D8 four-partition FineWeb Parquet run scaled from 52.86 seconds at jobs=1 to 43.32 seconds at jobs=2 and 40.67 seconds at jobs=4. That curve isolated the finalized-package Parquet destination boundary. After D8 replaced it with generic staged ingress, the same workload completes in 21.66/17.56/18.05 seconds at jobs 1/2/4. Jobs=2 is now the measured full-path knee, 1.234x jobs=1; jobs=4 regresses 2.8% against jobs=2.

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

## Post-D8 terminal scaling and identity addendum

D8 replaced the finalized-package boundary that defined the original C4 knee. The first current `arrow_ipc_to_parquet@5` path ran the same four-partition, 8,590,037,948-byte FineWeb workload at jobs=4 in 18.36 seconds, with 17.138 seconds of package execution overlapped by 16.762 seconds of destination ingress and 71.4 milliseconds of final binding/receipt work. It wrote 14,370,730,688 destination bytes in 58 deterministic objects while preserving 460 segment acknowledgements, held peak RSS to 1,462,665,216 bytes, left no attempt staging, verified the receipt, and committed the checkpoint. Complete-wall output was 746.5 MiB/s, or 0.779x the favorable 958.4 MiB/s two-writer same-FineWeb/same-policy reference. Four destination writers and a jobs=8 run both regressed or failed to improve wall. The full procedure and falsification history are in `.10x/evidence/2026-07-15-p3-d8-parquet-staged-ingress.md`.

A fresh isolated jobs 1/2/4 curve at commit `8e043953` then exercised that staged path from clean state/package/destination roots for every cell. All cells processed 4,234,560 rows into 460 segments and wrote approximately 8.2 GiB of package artifacts plus 13 GiB of destination artifacts:

| Jobs | Wall | User | System | Mean process cores | Peak RSS | Voluntary switches | Involuntary switches | Speedup |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 21.66 s | 49.36 s | 46.17 s | 4.410 | 834,732,032 B | 7,394 | 2,877,566 | 1.000x |
| 2 | 17.56 s | 50.86 s | 40.99 s | 5.231 | 1,177,468,928 B | 6,692 | 2,278,083 | 1.234x |
| 4 | 18.05 s | 51.27 s | 32.73 s | 4.654 | 1,417,314,304 B | 6,932 | 1,669,326 | 1.200x |

Mean process cores is `(user + system) / wall`; it includes native codec, filesystem, and kernel work rather than pretending the configured leaf-work ceiling is a process-wide thread count. Jobs=2 is the measured current knee: jobs=4 regresses 2.8%, and the prior jobs=8 plus four-destination-writer controls also failed to improve wall. Source-read/decode/validation/package-execution/destination-ingress durations were respectively 7.157/16.849/0.303/20.515/16.551 seconds at jobs=1, 7.940/0.463/0.208/17.342/16.979 seconds at jobs=2, and 9.399/0.333/0.218/17.613/17.267 seconds at jobs=4. Concurrent phase durations are additive, which is why they may exceed complete wall.

The generic scheduler report is now retained by `ExecutionServices` instead of being discarded at each scope join. Permanent format and destination cells record submitted/completed/cancelled/failed task totals, peak CPU slots, task queue wait, run-work ceiling/acquisitions/peak active/permit wait, canonical-frontier wait, prefetched batches, discarded prefetched batches, and peak ready partitions. Timing and task-report aggregation use the existing phase-measurement gate; an uninstrumented run does not call the monotonic clock at each frontier wait or lock the report accumulator at scope joins. The jobs 1/2/auto/4 matrices assert exact effective jobs 1/2/4/4, host/run-work peaks never above their authorities, and zero discarded prefetched batches. The full-scan matrix has no source retries; the generated failure/retry laws remain owned by accepted C1-C3 evidence. This supplies scheduler-overhead, permit, frontier, speculative-waste, retry, and nested-oversubscription evidence without any source- or destination-specific instrumentation.

The permanent destination matrix now executes the ordinary receipt/checkpoint path at jobs 1/2/auto/4 on an explicit four-slot host and requires exact effective jobs 1/2/4/4. Besides package hash, segment acknowledgements, state segments, partition count, and rows, it compares a destination-neutral logical receipt and the canonical logical Parquet manifest/digest. That projection retains destination object keys, content hashes, segment offsets, schema, disposition, and counts while excluding only wall-clock and physical transport-generation fields. The complete fast lab runner currently passes 11 tests with the live PostgreSQL destination cell intentionally opt-in; the separate live PostgreSQL run already recorded in the owning ticket passed all four jobs modes.

## What it supports or challenges

- It supports production partition fan-out: jobs=2 and jobs=4 both improve the same large governed run, and the jobs=4 package phase is only 18% of wall time.
- The original curve named finalized-package Parquet destination work as 81% of jobs=4 wall. D8 removed that serial boundary; the current complete command reaches 0.779x the favorable same-data reference and the fresh curve names jobs=2 with two destination writers as the useful concurrency knee on this host.
- It supports bounded memory for this 8 GiB input and jobs range, but does not by itself prove the 100 GiB/1 TiB constant-memory law.
- The permanent fixed-package-id matrix in `.10x/tickets/done/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md` proves package/segment/receipt/state plus logical destination receipt/manifest identity across jobs. Timed CLI package hashes intentionally differ because normal CLI package ids include run identity.

## Limits

This is one warm local APFS host, one repeated-content FineWeb object, four file partitions, and one filesystem Parquet destination. Hard links mean source pages can share the OS cache; the package and destination output is real. The result does not claim network, cold-cache, or more-than-four-partition scaling. The initial and current jobs curves are not variance-aware baselines; later repetitions test liveness but do not replace the lab's median-of-N throughput baselines. Runnable versus blocked operating-system thread time is approximated by process user/system/wall counters rather than exported per task; queue/frontier wait, permits, speculative waste, retry evidence, phase attribution, memory, and OS context switches are now explicit at their respective authorities.
