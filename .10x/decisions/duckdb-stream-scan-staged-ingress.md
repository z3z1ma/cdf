Status: active
Created: 2026-07-18
Updated: 2026-07-18

# DuckDB stream-scan staged ingress

## Context

P3 G4 isolated the remaining TLC DuckDB envelope miss to DuckDB/package materialization, not remote transport. On the dedicated EC2 benchmark host (`host-class-95da083e15eebd1c`), current CDF local TLC-to-DuckDB is `33.955522533s`; persistent DuckDB Arrow appender diagnostics are `31.831857687s`; raw C data-chunk append is `31.998755593s`; and native DuckDB Parquet ingest without CDF row provenance is `4.174575150s`.

The decisive lab result is DuckDB Arrow stream-scan materialization with the existing `_cdf_row_key` payload column preserved. Under bounded resource controls (`threads=16`, `memory_limit=1GiB`, `max_temp_directory_size=1GiB`), the synthetic TLC-shaped stream materialized 41,169,720 rows in median `5.111650191s`, peak RSS about `1.60GiB`. With `threads=1`, the same stream was `29.027263185s`, proving the native scanner's internal parallelism is the performance lever.

The public `duckdb` Rust binding's Arrow table function retains record batches in a process-global store and is not an acceptable production path for CDF's bounded memory model. The binding also does not expose the raw connection handle required to call DuckDB's C Arrow stream-scan API on the same transaction as CDF mirror writes. A production change must therefore remain destination-crate-owned and must not introduce generic runtime branches or package identity churn.

## Decision

DuckDB staged ingress may add a destination-owned stream-scan bulk path for eligible append/replace commits. The path MUST:

- preserve the existing logical and physical `_cdf_row_key` provenance contract unless a later decision supersedes it;
- expose CDF's durable staged segment stream to DuckDB as an Arrow C stream owned by `cdf-dest-duckdb`;
- keep all DuckDB-specific raw C API and `unsafe` code inside `cdf-dest-duckdb`;
- use the generic `BulkPathPreparationInput.commit` and destination runtime capabilities to select eligible paths, never a runtime destination-name branch;
- record a new bulk path id/version/evidence version when selected;
- derive DuckDB internal parallelism from execution/host capability or explicit destination resource knobs, not a hidden hard cap;
- preserve transaction, receipt, row counts, `_cdf_segments`, duplicate token, rollback, and correction semantics.

Because `duckdb_arrow_scan` is deprecated in the pinned DuckDB C API, production use is permitted only behind this destination-owned boundary with pinned-version tests and a safety comment. If the API disappears or semantics change, the path fails preparation and the appender path remains the measured compatibility path until a replacement raw-handle/upstream API is available.

Merge remains on the existing Arrow appender path until a stream-scan merge staging implementation has equivalent correctness and performance evidence. That is a destination-local path selection by disposition, not generic fallback. Runtime fallback after payload acceptance remains forbidden.

## Alternatives considered

- Use the Rust binding's `ArrowVTab`: rejected because it retains batches in a process-global store and violates bounded memory.
- Remove `_cdf_row_key` and use DuckDB `rowid`: rejected for this work because the `_cdf_row_key` stream-scan control is already faster than needed and preserves the current provenance contract.
- Expose a raw DuckDB connection handle from generic runtime: rejected as a destination-specific leak.
- Rewrite all DuckDB destination behavior around a raw connection wrapper immediately: rejected as too broad for the measured append/replace envelope fix; raw use should be limited to the stream-scan path unless evidence demands a full rewrite.
- Raise current appender threads by default: rejected by prior EC2 controls; appender-shaped ingestion did not improve enough and overdrive can regress completion.

## Consequences

Adding the stream-scan path is a pre-production destination storage/execution change. No old DuckDB target compatibility is required. Conformance must prove that visible payload columns, `_cdf_row_key`, `_cdf_segments`, receipt verification, duplicate replay, rollback, append, replace, and correction readback still agree with the existing contract. G4 closure still requires full CDF local and HF TLC EC2 cells, not only the synthetic lab reference.
