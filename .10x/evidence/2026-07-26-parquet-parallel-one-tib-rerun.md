Status: recorded
Created: 2026-07-26
Updated: 2026-07-26
Relates-To: .10x/tickets/done/2026-07-26-parquet-parallel-object-encoding.md

# Parallel Parquet one-TiB acceptance rerun

## Observation

Release revision `e74bb2fde18189eb23ad16ea59c5ef92cfac688e` completed the same
1,108,930,093,056-logical-byte (1.0086 TiB), 5,435,817,984-row, 1,024-file,
5,120-segment governed Parquet-to-package-to-Parquet workload used by the 2026-07-25 baseline.
The complete timed `cdf run` took 499.07 seconds and averaged 678% CPU. This is 2.222 GB/s
(2.069 GiB/s), 10.892 million rows/s, and 5.40x the baseline's 411.5 MB/s / 2,694.863 seconds.

The host has eight physical cores and 16 SMT logical CPUs. User plus system work totaled
3,384.88 CPU-seconds, equivalent to 6.78 fully occupied cores over the run and 84.8% of the
physical-core roofline. Periodic samples observed a stable 6.8--7.0 equivalent cores through the
main segment-production interval, rather than a startup burst. The former baseline consumed
3,475.37 CPU-seconds. Total CPU work therefore fell 2.6% while wall time fell 81.5%; the speedup
comes from exposing existing work to the host, not doing more work or weakening evidence.

Peak process RSS was 3,923,718,144 bytes under the unoverridden 4 GiB process policy. Managed
memory peaked at 3,163,272,636 of 3,650,722,202 bytes, returned to zero, and recorded no spill.
The enclosing 5 GiB cgroup recorded no OOM or OOM-kill event. Its `max` counter reflects
file-cache reclaim at the enforcement boundary; the product's independent RSS and managed-pool
acceptance checks both passed.

The run committed checkpoint
`checkpoint-stress-rows-99009-1785104737945404221`, published receipt
`parquet:rows:sha256:dafc0cbfeabc0bd04c19c71d079ca48997c3dda277d8615d90ae10ca66ac220d`,
and produced package
`sha256:dafc0cbfeabc0bd04c19c71d079ca48997c3dda277d8615d90ae10ca66ac220d`.
The independent package verifier checked all 5,135 package files and returned the same hash.

Before the long run, an interleaved median-of-three comparison used the same production release
shape on a 10-object, 11.23 GB logical fixture:

| Physical path | Median wall | Average CPU near median | Destination Parquet bytes |
|---|---:|---:|---:|
| Zstd level 1 | 6.95 s | 499% | 5,723,465 |
| LZ4 raw | 7.99 s | 525% | 26,291,265 |
| Snappy | 8.33 s | 546% | 73,305,407 |
| Uncompressed | 14.41 s | 382% | 1,123,053,555 |

The ordinary one-object control took a 1.16-second Zstd median and a 1.57-second Snappy median,
both materially below the retained 2.407-second parent baseline. This selected Zstd level 1 as
the default compiled physical path without sacrificing the ordinary path. None, Snappy, LZ4 raw,
and Zstd remain explicit plan-recorded choices.

## Procedure

The retained benchmark host was EC2 instance `i-0b65f54b763e1e106`, a `c7i.4xlarge` with
16 logical CPUs, eight physical Intel Xeon Platinum 8488C cores, 32 GiB RAM, and a 250 GiB gp3
root volume configured for 16,000 IOPS and 1,000 MiB/s. The optimized binary SHA-256 was
`a96954733840fba925409bab67fdfd6f2b4c543dfccbe243ce51de2843c6d333`.

The exact acceptance invocation was:

```text
systemd-run --user \
  --property=WorkingDirectory=/home/ec2-user/cdf-bench/repo \
  --property=MemoryMax=5G \
  --property=MemorySwapMax=0 \
  --setenv=CDF_STRESS_CDF=target/release/cdf-e74bb2fd \
  tools/run-constant-memory-stress.sh \
    target/cdf-benchmarks/constant-memory-1t-e74bb2fd \
    1024 1073741824 default
```

`default` deliberately omitted `--memory-budget`. Fixture generation and the second independent
package-verification command were outside the timed `cdf run`; inventory, decode, validation,
normalization, canonical persistence and hashing, Parquet destination commit, receipt
verification, and checkpoint commit were inside it.

Raw observations:

- `.10x/evidence/.storage/2026-07-26-parquet-parallel-1t-summary.json`
- `.10x/evidence/.storage/2026-07-26-parquet-parallel-1t-run.json.gz`
- `.10x/evidence/.storage/2026-07-26-parquet-parallel-1t-process-time.txt`
- `.10x/evidence/.storage/2026-07-26-parquet-parallel-1t-package-verify.json`
- `.10x/evidence/.storage/2026-07-26-parquet-parallel-1t-generator.json`
- `.10x/evidence/.storage/2026-07-26-parquet-parallel-1t-generator-time.txt`
- `.10x/evidence/.storage/2026-07-26-parquet-parallel-1t-cpu-samples.tsv`
- `.10x/evidence/.storage/2026-07-26-parquet-parallel-1t-host.txt`
- `.10x/evidence/.storage/2026-07-26-parquet-parallel-1t-build.txt`
- `.10x/evidence/.storage/2026-07-26-parquet-parallel-codec-multi.tsv`
- `.10x/evidence/.storage/2026-07-26-parquet-parallel-codec-control.tsv`

## What it supports or challenges

The observation supports three claims together: destination-stage queue depth no longer limits
run-wide jobs; deterministic Parquet object groups actually encode concurrently; and the default
physical output path no longer converts that CPU parallelism into an uncompressed disk
bottleneck. It also preserves the constant-memory law and proves that package, receipt, and
checkpoint correctness survive the parallel topology.

The near equality in total CPU-seconds between baseline and current runs is especially strong
evidence for the topology diagnosis. CDF now performs approximately the same governed work at
327.6 MB per CPU-second versus 319.1 MB per CPU-second before, but schedules that work across
6.78 equivalent cores instead of 1.28.

## Limits

The generator intentionally uses repeated content and hard links. CDF still decodes, validates,
normalizes, packages, hashes, and commits every logical row, so this is a valid logical-work,
CPU-scheduling, lifecycle, and constant-memory comparison against the exact former baseline.
It is not a unique-source-byte or cold-network roofline. Content addressing and Zstd compress the
destination to 46,365,275 physical bytes, so this workload no longer measures sustained
destination-device bandwidth.

The 678% CPU figure is Linux's process average on an eight-physical-core/16-SMT host. It proves
near physical-core saturation, not 16 independent physical cores. Other schemas, compression
ratios, devices, and destinations can move the knee. The explicit compression paths remain
available because incompressible or externally constrained workloads may select differently.
