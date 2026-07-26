Status: recorded
Created: 2026-07-25
Updated: 2026-07-25
Relates-To: .10x/tickets/done/2026-07-11-p3-f4-one-tb-memory-closeout.md

# P3 F4 one-TiB constant-memory closeout

## Observation

The current optimized `cdf` binary completed 1,108,930,093,056 logical input bytes
(1.0086 TiB), 5,435,817,984 rows, 1,024 files, and 5,120 canonical segments through the complete
governed Parquet-to-package-to-Parquet path in 2,694.863 seconds. Logical throughput was
411.5 MB/s and 2.017 million rows/s.

Peak process RSS was 2,035,298,304 bytes (1.896 GiB) under the unoverridden 4 GiB process policy.
Managed peak was 2,490,367,380 bytes of the 3,650,722,202-byte pool; all managed ownership returned
to zero. The enclosing 5 GiB cgroup recorded `oom = 0`, `oom_kill = 0`, and
`oom_group_kill = 0`. No algorithmic spill was required, so `spill_bytes` and `spill_count`
correctly remained zero; F3's separately forced spill/exhaustion laws remain the spill authority.

The run committed checkpoint
`checkpoint-stress-rows-66921-1785049966748480525`, published receipt
`parquet:rows:sha256:078686f3b6d38b4416c695c6fd585b3d35bb8458da26b7cba6785f9ebf3f21e0`,
and independently verified package
`sha256:078686f3b6d38b4416c695c6fd585b3d35bb8458da26b7cba6785f9ebf3f21e0`.
The second package verifier checked 5,135 identity files and returned success.

The same run was sampled after 1,668 of the expected 5,120 canonical segments were durable.
CDF held 27 file descriptors and 47 threads, `VmHWM` was 1,980,008 KiB, current RSS was
1,784,584 KiB, and the enclosing cgroup had recorded no OOM or OOM-kill event. This is an
intermediate liveness/cardinality observation, not the terminal scale result.

The current release PostgreSQL binary-COPY path processed 524,288 rows through a real temporary
PostgreSQL server at 1,801,714 rows/s, 3.00x the retained CSV control. A warm
`/usr/bin/time -lp cargo test` invocation completed in 5.64 seconds with 164,904,960 bytes maximum
resident set size for the Cargo/test process and 139,805,464 bytes reported peak memory footprint.

An independent cold `cdf schema discover` over the same 1,024-file inventory completed in
0.36 seconds at 106,496 KiB peak RSS. Its evidence reports `all_files`,
`format_metadata`, 1,024 matched/selected files, zero unobserved files, 34,863,104 footer bytes,
and zero record payloads. It wrote no schema snapshot, package, destination, checkpoint, or
lockfile.

The already-accepted same-path scaling authority remains the C4/D8 FineWeb curve: the staged
Parquet destination improved 21.66/17.56/18.05 seconds at jobs 1/2/4, making jobs=2 the measured
host knee; its full path reached 0.779x the favorable same-data reference, and the isolated writer
reached 0.786x the raw sequential-write roofline. Jobs=4 and four destination writers were
measured regressions rather than silently higher defaults. F4 reuses that named device/concurrency
evidence instead of conflating one long memory law with a new scaling experiment.

F3's accepted focused matrix remains the native-envelope authority outside this Parquet cell:
streamed object-store gzip NDJSON held transport/transform/codec backpressure and cancellation,
HTTP gzip stopped before full download and released managed memory, and forced/exhausted spill
paths completed or failed cleanly. Those observations are recorded in
`.10x/evidence/2026-07-25-p3-f3-constant-memory-matrix.md`; F4 does not duplicate them at 1 TiB.

## Procedure

The retained EC2 benchmark host is a `c7i.4xlarge` with 16 logical CPUs, 32 GiB RAM, and a tuned
2 TiB gp3 root volume. The clean fat-LTO release products run inside:

```text
systemd-run --user --wait --collect \
  --property=MemoryMax=5G \
  --property=MemorySwapMax=0 \
  bash -lc \
  'cd /home/ec2-user/cdf-bench/repo &&
   tools/run-constant-memory-stress.sh
     target/cdf-benchmarks/f4-1t-default 1024 1073741824 default'
```

Passing `default` deliberately omits `--memory-budget`; the product therefore resolves its
unoverridden 4 GiB process authority. The extra cgroup GiB is enforcement/file-cache headroom, not
a relaxed CDF process budget. The runner independently rejects process RSS above the resolved
budget and managed memory above the managed pool.

The terminal cgroup sample recorded `max = 10,201,547` but `oom = 0`, `oom_kill = 0`, and
`oom_group_kill = 0`. Those `max` events are page-cache reclaim against the outer 5 GiB
enforcement envelope; the product process remained below its independent 4 GiB RSS authority.

Raw retained observations:

- `.10x/evidence/.storage/2026-07-25-p3-f4-ec2-1t-summary.json`
- `.10x/evidence/.storage/2026-07-25-p3-f4-ec2-1t-run.json.gz`
- `.10x/evidence/.storage/2026-07-25-p3-f4-ec2-1t-package-verify.json`
- `.10x/evidence/.storage/2026-07-25-p3-f4-ec2-1t-process-time.txt`
- `.10x/evidence/.storage/2026-07-25-p3-f4-ec2-1t-cgroup-last.txt`
- `.10x/evidence/.storage/2026-07-25-p3-f4-ec2-1t-generator.json`
- `.10x/evidence/.storage/2026-07-25-p3-f4-ec2-discovery-1024.json`
- `.10x/evidence/.storage/2026-07-25-p3-f4-ec2-discovery-1024-time.txt`

The PostgreSQL observation used:

```text
CARGO_BUILD_JOBS=12 /usr/bin/time -lp \
  cargo test -p cdf-dest-postgres --release \
  live_binary_copy_is_at_least_twice_csv --locked -j 12 \
  -- --ignored --nocapture
```

## What it supports or challenges

The terminal result proves the default-budget 1 TiB law and demonstrates stable process RSS
through the complete product path. The intermediate sample independently challenges any claim
that open handles, threads, or RSS scale with the 1,668 already-durable segments. The 1,024-file
cold discovery observation directly closes candidate/evidence cardinality at this envelope rather
than inferring it from the declared-schema scale run.

The PostgreSQL observation bounds the current synchronous client, binary encoder, protocol
buffers, and temporary server within the named row/host envelope while preserving the performance
claim. The generated owner matrix has no open row and its closure mode validates every evidence
path.

## Limits

The repeated-content generator uses hard links. CDF still decodes and commits every logical
partition, while the content-addressed destination may reuse identical objects. The observation
is therefore a process-memory, lifecycle, metadata-cardinality, and logical-work law—not a unique
source-byte storage-capacity or cold-device-throughput benchmark.

The PostgreSQL measurement is a local macOS loopback observation over a 524,288-row benchmark
schema, not a remote PostgreSQL or arbitrarily wide-field memory guarantee. Native-library and
metadata owners are classified only within the exact code/envelope named by the generated owner
matrix. Current file-position/state/ack semantic arrays remain one record per identity; the
roadmap explicitly forbids extrapolating this 1,024-file/5,120-segment measurement into a
million-file or unbounded-horizon constant-metadata claim.

The C4/D8 curve is one Apple M5 Pro/APFS host and a four-partition FineWeb workload; it does not
claim that jobs=2 is a portable constant or that the 1 TiB memory cell independently measures a
device roofline.
