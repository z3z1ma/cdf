Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Terabyte-scale performance envelope and measurement doctrine

## Context

`VISION.md` Chapter 6 already fixes CDF's runtime architecture: separate I/O and CPU execution resources, byte-bounded channels, one memory ledger, recorded adaptive batches, explicit boundedness, and deterministic replay. The current implementation does not yet realize that architecture and materializes data at multiple boundaries. Bare throughput claims would encourage benchmark gaming and ticket-local shortcuts, while absolute-only targets would be meaningless across host classes.

## Decision

P3 implements Chapter 6 without weakening package identity, validation, receipts, the checkpoint gate, replay, or deterministic assembly. Performance is measured against same-host rooflines and reference implementations. Every optimization requires a recorded before/after result on the shared harness.

The close envelope is:

| Path | Required target |
|---|---|
| Parquet file/glob to package | at least 0.7 times raw arrow-rs Parquet and 1.5 GB/s aggregate on the reference NVMe host; row-group parallel |
| CSV to package | at least 0.6 times raw arrow-csv and 400 MB/s aggregate |
| NDJSON/JSON, including streamed gzip/zstd | 300-500 MB/s aggregate and at least 3 times the current DOM path per core |
| Contract validation | at least 1 GB/s per core on 64k-row batches |
| Package build | at least 70% of sequential-write roofline; hashing at most 5% of wall time |
| Package to DuckDB | Arrow-native append at least 1 million TLC-schema rows/s and 5 times the current scalar appender |
| Package to local Postgres | binary COPY at least 2 times the current CSV COPY rows/s |
| Package to Parquet | streaming row-group writer at least 60% of device-write roofline |
| Full-year TLC HTTPS glob to DuckDB | within 1.5 times download plus DuckDB-native ingest, with an I/O-dominated profile |
| 1 TB synthetic glob to Parquet | completes under the configured memory budget with linear scaling until device saturation |

CDF correctness/evidence overhead MUST be no more than 15% of equivalent raw read-plus-write when the program first establishes the envelope and no more than 10% at program close. If a correctness mechanism exceeds the budget, it is optimized; it is not removed or weakened.

Peak RSS MUST be bounded by configuration rather than input size. The default runtime budget is 4 GiB. The permanent stress law uses a generated 100 GB input under a 2 GiB budget and requires completion within the ceiling, including observable spill. A budget smaller than one legal batch MUST fail cleanly with a `Data` error and remediation rather than OOM.

Each CI host class records device, CPU, memory, OS, toolchain, warm/cold mode, and reference-decoder versions. Regression gates use median-of-N and fail when a comparable metric regresses by more than 10%, with variance and host-class mismatch reported instead of silently compared.

## Alternatives considered

- Absolute targets only were rejected because they cannot compare heterogeneous CI and developer hosts.
- Roofline ratios only were rejected because a very slow host or reference path could satisfy a ratio while missing the product ambition.
- Tuning before the lab was rejected because it destroys the before picture and cannot establish causality.
- An ephemeral fast path without packages or receipts was rejected because it would create a second semantic system.
- Weaker hashes, reduced validation, or checkpoint shortcuts were rejected because correctness is the product contract, not optional overhead.

## Consequences

WS-L runs first and alone until the baseline evidence exists. Later workstreams may change execution topology and implementation but not observable semantics. Results are publishable only with environment and bias labels. New dependencies, allocators, `unsafe`, or artifact changes require their existing supply-chain and decision gates. This decision should be reopened only if reproducible lab evidence shows a target is physically impossible under the named roofline or a correctness mechanism cannot fit after architectural optimization.
