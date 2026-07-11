# CDF Terabyte-Scale Performance Envelope

> **PRE-OPTIMIZATION BASELINE — failures and unavailable cells are intentional evidence, not green performance claims.**

This document is generated from the machine report; edit its inputs, not this file.

## Evidence authority

- Report: `sha256:534bc6ddf4ead5471e218f3630ef084f7442a9a6ebb3fc96bbbe5018116e4c66`
- Host class: `host-class-f4bf4d1c46a93156`
- Host: aarch64 / 18 logical cores / macos 26.5.1 / Rust rustc 1.96.1 (31fca3adb 2026-06-26)
- Effective CPU: supported via process-available-parallelism: 18 logical, quota unbounded, affinity 18
- Effective memory: unavailable via container-memory-overlay: no container memory overlay on this platform
- Storage: supported via filesystem-target-probe: unknown / apfs / apfs-storage-class
- Evidence record: `.10x/evidence/2026-07-11-p3-l5-preoptimization-baseline.md`

## Performance envelope

| Workload | Target | Observation | Roofline ratio | Evidence overhead | Peak RSS | Status |
|---|---:|---:|---:|---:|---:|---|
| Parquet file/glob → package | ≥0.7× raw Arrow; ≥1.5 GB/s | — | — | — | — | unavailable: full year TLC acquisition manifest is not present on this host |
| CSV → package | ≥0.6× raw Arrow; ≥400 MB/s | — | — | — | — | unavailable: TPC H SF10 generated dataset is not present on this host |
| NDJSON/JSON → package | ≥300–500 MB/s; ≥3× DOM path/core | 0.75 MiB/s | 0.009× | +10574.8% | 16.20 MiB | observed |
| Contract validation | ≥1 GB/s/core at 64k rows | — | — | — | — | failed: dedicated P3 vector validation baseline runner is not implemented |
| Package build | ≥70% device write; hash ≤5% wall | 0.24 MiB/s | — | — | 15.78 MiB | observed |
| Package → DuckDB | ≥1M rows/s; ≥5× scalar appender | 0.17 MiB/s | — | — | 43.83 MiB | observed |
| Package → Postgres | binary COPY; ≥2× CSV COPY | — | — | — | — | unavailable: disposable Postgres benchmark service is not configured |
| Package → Parquet | ≥60% device write roofline | 0.21 MiB/s | — | — | 17.81 MiB | observed |
| Full-year TLC HTTPS → DuckDB | ≤1.5× download + native ingest | — | — | — | — | unavailable: full year TLC acquisition and live network benchmark are not enabled |
| 1 TB synthetic → Parquet | default budget; stable RSS; linear scaling | — | — | — | — | failed: preoptimization materializing data plane cannot safely execute the 100 GiB fixed budget stress law |

## Bias and unavailable evidence

- `raw_arrow_ndjson` (warm): observed; bias: omits_cdf_evidence: omits contract validation package hashing receipts and checkpoints
- `json_ndjson_to_package` (warm): observed; bias: includes_cdf_evidence: includes decode validation normalization package encode hash and finalize; fixture_scale: medium fixture exposes current costs but is not a large scale claim
- `package_build` (warm): observed; bias: includes_legacy_setup: legacy compatibility case includes fixture and plan setup
- `duckdb_commit` (warm): observed; bias: includes_legacy_setup: legacy compatibility case includes package fixture construction
- `parquet_destination` (warm): observed; bias: includes_legacy_setup: legacy compatibility case includes package fixture construction
- `legacy_tiny_startup_e2e` (warm): observed; bias: includes_legacy_setup: startup case intentionally includes child fixture compile package destination and checkpoint
- `tlc_parquet_to_package` (warm): unavailable: full year TLC acquisition manifest is not present on this host; bias: none recorded
- `tpch_csv_to_package` (warm): unavailable: TPC H SF10 generated dataset is not present on this host; bias: none recorded
- `validation_kernel` (warm): failed: dedicated P3 vector validation baseline runner is not implemented; bias: none recorded
- `postgres_commit` (warm): unavailable: disposable Postgres benchmark service is not configured; bias: none recorded
- `tlc_e2e_duckdb` (warm): unavailable: full year TLC acquisition and live network benchmark are not enabled; bias: none recorded
- `constant_memory_stress` (warm): failed: preoptimization materializing data plane cannot safely execute the 100 GiB fixed budget stress law; bias: none recorded

## Profiles

- `json_ndjson_to_package`: [artifact](../.10x/evidence/.storage/p3-baseline-cdf-ndjson-sample.txt)
