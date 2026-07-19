Status: recorded
Created: 2026-07-19
Updated: 2026-07-19

# P3 B4 CSV envelope on the controlled EC2 host

## Observation

On the dedicated `c7i.4xlarge` benchmark host, the existing sequential CDF CSV path processes one 232,675,583-byte, 8,000,000-row file into a complete canonical package in a median 1.064293 seconds. The corrected raw Arrow CSV reference, including the same explicitly bounded 1,000-record inference pass, has a median 0.723953 seconds. CDF therefore reaches 0.680x the same-host Arrow reference and clears B4's ratio floor of 0.6x.

Four identical local files execute with effective jobs four and a stable package hash, but complete in a median 3.622013 seconds for 930,702,332 physical bytes (257.0 MB/s decimal). This misses the 400 MB/s aggregate ambition. The source frontier records four active/ready partitions and three prefetched batches, while the canonical partition contract intentionally stops polling later partitions after one retained batch. Because CSV currently exposes one whole-file decode unit, it cannot keep multiple file decoders running behind the canonical head. Parquet avoids this boundary by exposing row groups as nested units. B4's already-scoped safe CSV decode units are therefore the direct owner; reopening the source-neutral scheduler would weaken its bounded canonical frontier without solving the format's missing unit grain.

A quote-aware local replay candidate was then measured at clean revision `a8a5fa3e5549077c794f3795a0816eb1fb551718`. It first scanned each local file for safe record boundaries and then decoded 32 MiB exact ranges through the generic unit scheduler. The candidate is rejected and reverted. On one file, explicit sequential decode completed in 1.037673 seconds while automatic units took 1.819529 seconds, a 75.3% regression, and source-read accounting doubled from 232,675,583 to 465,351,166 bytes. On four files, sequential completed in 3.511328 seconds while units took 3.598940 seconds, a 2.5% regression. More importantly, unit boundaries changed segment row counts and package identity (`sha256:d596...` versus `sha256:d536...` for one file), violating canonical jobs/strategy invariance. The candidate improved no measured path and cannot be retained even as a knob because its execution strategy changes identity-bearing segmentation.

## Procedure

- Host: the L6-controlled EC2 `c7i.4xlarge`, 16 logical CPUs, gp3 configured at 16,000 IOPS and 1,000 MiB/s.
- Clean measured revision: `d6c829e59b16ee8c2ec7719d572828b6bccf21db`.
- Toolchain: Rust `1.97.1`; DuckDB used the downloaded-prebuilt linkage.
- Fixture: generated CSV columns `id,active,category,amount`; 8,000,000 data rows; 232,675,583 bytes per file.
- Raw reference: `cdf-p3-lab reference-worker`, Arrow CSV 59.1.0, 65,536-row batches, header enabled, `infer_rows = 1000`, three warm samples.
- CDF single-file: `cdf-p3-lab cdf-file-package-worker`, jobs/host slots 16, complete package build, three warm samples.
- CDF four-file: four hard-linked fixture files, jobs/host slots four, complete package build, three warm samples.
- Rejected unit candidate: same host and fixture, release fat-LTO build at `a8a5fa3e`, explicit `parallel_decode = off` versus `auto` with 32 MiB units and an 8 MiB record bound; one diagnostic sample per shape was sufficient because both performance and identity moved in the wrong direction.

Raw artifacts:

- `.10x/evidence/.storage/2026-07-19-p3-b4-csv-reference-bounded-runs.jsonl`
- `.10x/evidence/.storage/2026-07-19-p3-b4-csv-cdf-bounded-runs.jsonl`
- `.10x/evidence/.storage/2026-07-19-p3-b4-csv-multi-runs.jsonl`
- `.10x/evidence/.storage/2026-07-19-p3-b4-ec2-revision.env`
- `.10x/evidence/.storage/2026-07-19-p3-b4-ec2-build.env`

## What it supports or challenges

- Supports the existing sequential codec's ≥0.6x same-host native-reference acceptance ratio.
- Supports jobs-invariant four-file package identity: every measured run produced `sha256:9a989ef8393a6a3ed954bc6d979868b8e2b43d8759e7e6c990feea774fc0c8a1`.
- Challenges closure of the aggregate CSV target: 257.0 MB/s is below 400 MB/s.
- Identifies the missing safe intra-file decode-unit grain as the owner without changing generic canonical scheduling.
- Rejects a separate quote-aware boundary pre-scan followed by exact-range decode: it performs two reads, is slower for both tested shapes, and leaks unit boundaries into canonical package identity.

## Limits

This fixture contains no quoted newlines, malformed rows, alternate encoding, or compression. Hard links minimize device-read pressure after warmup. The comparison is package build versus raw Arrow decode, so CDF intentionally performs additional validation, canonical segmentation, compression, hashing, statistics, and finalization. The rejected candidate's single samples are falsification evidence, not a retained-path baseline. The evidence does not yet prove fixed-width throughput or RSS bounds. Any future parallel CSV design must perform one pass and preserve the canonical row-based segmentation contract across execution strategies.
