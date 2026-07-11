# CDF Terabyte-Scale Performance Envelope

> **PRE-BASELINE TEST FIXTURE — no performance claim is authorized until P3 L5 replaces this document from recorded evidence.**

This document is generated from the machine report; edit its inputs, not this file.

## Evidence authority

- Report: `sha256:ec1c1216b0c68e6167fc32b6e26804190608f52def93f9f69187eb68d1b345f1`
- Host class: `host-class-a370b3a8008eeef0`
- Host: aarch64 / 18 logical cores / macos fixture / Rust 1.96.1
- Effective CPU: supported via fixture: 18 logical, quota unbounded, affinity 18
- Effective memory: unavailable via fixture: no container memory authority in fixture
- Storage: supported via fixture: nvme / apfs / local-nvme-class

## Performance envelope

| Workload | Target | Observation | Roofline ratio | Evidence overhead | Peak RSS | Status |
|---|---:|---:|---:|---:|---:|---|
| Parquet file/glob → package | ≥0.7× raw Arrow; ≥1.5 GB/s | — | — | — | — | unavailable: no report cell |
| CSV → package | ≥0.6× raw Arrow; ≥400 MB/s | — | — | — | — | unavailable: no report cell |
| NDJSON/JSON → package | ≥300–500 MB/s; ≥3× DOM path/core | — | — | — | — | unavailable: no report cell |
| Contract validation | ≥1 GB/s/core at 64k rows | — | — | — | — | unavailable: no report cell |
| Package build | ≥70% device write; hash ≤5% wall | — | — | — | — | unavailable: no report cell |
| Package → DuckDB | ≥1M rows/s; ≥5× scalar appender | — | — | — | — | unavailable: no report cell |
| Package → Postgres | binary COPY; ≥2× CSV COPY | — | — | — | — | unavailable: no report cell |
| Package → Parquet | ≥60% device write roofline | — | — | — | — | unavailable: no report cell |
| Full-year TLC HTTPS → DuckDB | ≤1.5× download + native ingest | — | — | — | — | unavailable: no report cell |
| 1 TB synthetic → Parquet | default budget; stable RSS; linear scaling | — | — | — | — | unavailable: no report cell |

## Bias and unavailable evidence

- `legacy_medium_ndjson_package` (warm): observed; bias: includes_evidence: CDF includes validation, package hashing, and finalization
- `tlc_polars_reference` (uncontrolled): unavailable: Polars executable is not installed; bias: omits_cdf_evidence: Reference scan omits package, receipt, and checkpoint work

## Profiles

No profile artifacts are attached to this report.
