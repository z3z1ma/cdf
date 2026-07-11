Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md, .10x/tickets/2026-07-10-p3-terabyte-scale-program.md

# P3 pre-optimization performance baseline

## What was observed

The first P3 baseline was executed before WS-A through WS-H data-plane optimization on host class `host-class-f4bf4d1c46a93156`: Apple M5 Pro, 18 logical CPUs, macOS 26.5.1, Rust 1.96.1. The authoritative release-mode report contains nine isolated process samples for every observed case and retains each sample's wall time, peak RSS, physical/logical byte counts, rows, and available phase telemetry.

Authoritative raw report: `.10x/evidence/.storage/p3-baseline-macos-ef3d84f6.json`

Report digest: `sha256:534bc6ddf4ead5471e218f3630ef084f7442a9a6ebb3fc96bbbe5018116e4c66`

An earlier report at `.10x/evidence/.storage/p3-baseline-macos-13aaefad.json` is retained as failed-attempt evidence. Its startup cell exposed a harness identifier-policy mismatch fixed in commit `ef3d84f6`; it is not baseline authority.

| Workload | Median wall time | Median throughput | Median peak RSS | Status |
|---|---:|---:|---:|---|
| Raw arrow-rs NDJSON reference | 1.543 ms | 80.06 MiB/s | 12.25 MiB | observed |
| CDF NDJSON to package | 164.721 ms | 0.750 MiB/s | 16.20 MiB | observed |
| Package build | 155.885 ms | 0.235 MiB/s | 15.70 MiB | observed |
| Package to DuckDB | 164.006 ms | 0.170 MiB/s | 43.48 MiB | observed |
| Package to Parquet | 133.029 ms | 0.210 MiB/s | 17.77 MiB | observed |
| Legacy tiny startup end to end | 297.237 ms | 0.00591 MiB/s | 45.16 MiB | observed |

The comparable NDJSON reference and CDF cases processed identical row counts and physical source bytes. CDF achieved `0.009x` the raw arrow-rs reference throughput, an observed wall-time overhead of `+10,574.8%`. Logical Arrow allocation differs and is reported separately; it is not used to pretend the physical workloads differ. This result is the pre-optimization before picture, not an acceptable envelope result.

Median CDF NDJSON phase durations were: decode 2.468 ms, validation/normalization 0.265 ms, segment encode 31.535 ms, persist/hash 12.499 ms, and package finalize 8.968 ms. The gap between summed instrumented phases and process wall time remains explicit uninstrumented/setup overhead for later decomposition.

Every ratified target row has an outcome. TLC Parquet, TPC-H CSV, Postgres, and live TLC end-to-end were unavailable because their datasets/services were not provisioned on this host. The vector validation cell failed because its dedicated P3 runner does not exist yet. The 100 GiB constant-memory cell failed without execution because the pre-optimization materializing plane cannot safely run it under the fixed budget. These cells remain visibly unavailable or failed in `docs/performance-envelope.md`.

## Representative profile

The retained macOS `sample` profile is `.10x/evidence/.storage/p3-baseline-cdf-ndjson-sample.txt`. It repeatedly executed the real release CDF NDJSON-to-package worker. The captured call tree places 321 of 439 samples beneath `PackageBuilder::write_segment_inner`; `File::sync_all` and directory synchronization account for prominent subtrees. This supports prioritizing the existing hash/package-I/O and streaming pipeline owners. It does not prove those samples' percentages generalize to larger files.

A flamegraph attempt was also made. It could not run because `xctrace` requires a full Xcode installation on this host. No synthetic flamegraph was substituted.

## Procedure

1. Built the `cdf-p3-lab` and benchmark worker binaries in release mode from the pre-optimization tree.
2. Executed the complete catalog with nine isolated process samples per runnable case.
3. Preserved failures and unavailable results instead of filtering them from the report.
4. Generated `docs/performance-envelope.md` from the report and ratified envelope specification.
5. Captured the repeated CDF workload with macOS `sample` and retained the raw text profile.
6. Installed the immutable content-addressed baseline and host-class index only after this evidence record existed.

## Triage reconciliation

- Batch sizing/segment coalescing: the observed path still emits high fixed segment/durability costs for a tiny fixture; A3 owns adaptive microbatches and canonical segmentation. The baseline does not isolate the batch-size curve.
- DuckDB bulk load: 0.170 MiB/s on the prepared tiny package, with compatibility/setup bias; D2/D5 own Arrow-native selection and large-fixture proof.
- Interop boundaries: no Python/subprocess/WASM copy-proof cell exists yet; H1-H5 own that explicit gap. No performance claim is made.
- Local partition parallelism: all baseline workers are sequential; C1-C5 own scaling and jobs-invariance proof.
- Native Parquet streaming write: 0.210 MiB/s on the prepared tiny package; D4/D5/F4 own streaming, large-file roofline, and constant-memory evidence.
- Package I/O/hashing: 0.235 MiB/s package build plus the sampled durability call tree identify this as a measured cost center; E1-E4 own hash-while-write and durability closeout.
- REST/JSON-to-Arrow: CDF NDJSON is 0.009x raw arrow-rs on identical rows/physical bytes; B5/G3/B13 own tape/streamed decode, overlap, and cross-format closure.
- Streaming package-to-destination commit: package and destination cases remain separately materialized and slow; A1/A5/D1-D5/F2 own bounded streaming and receipt/crash proof.

The active triage records remain evidence checklists until their implementation owners record the required before/after results; L5 does not prematurely close them.

## What this supports or challenges

This evidence supports releasing the WS-L stop-line: the before picture is immutable, reproducible on its named host class, and visibly incomplete where the environment or implementation is incomplete. It directly challenges any claim that the current data plane is already competitive or constant-memory.

## Limits

The observed fixtures are intentionally small and expose startup/fixed-cost behavior. They do not establish large-file steady-state throughput, multicore scaling, network saturation, terabyte memory behavior, or destination roofline ratios. Cold-cache control is best-effort on macOS. DuckDB and Parquet destination cases include compatibility-path setup bias. Missing enterprise datasets and services are failures of baseline coverage on this host, not evidence that the corresponding paths are fast or slow.
