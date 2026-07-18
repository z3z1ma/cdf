Status: recorded
Created: 2026-07-12
Updated: 2026-07-18
Relates-To: .10x/tickets/done/2026-07-11-p3-d5-bulk-path-matrix.md

# D5 named-host destination bulk matrix

## Observation

On the Apple M5 Pro/macOS host class `host-class-f4bf4d1c46a93156`, the current selected first-party paths produced:

- DuckDB `arrow_record_batch_appender`: 1,048,576 TLC-shaped rows in 113,058,416 ns: 9,274,639 rows/s, 23.77x the removed scalar control and 87.5% of the direct Arrow appender control.
- PostgreSQL `copy_binary`: 31,771,822 encoder rows/s versus 12,006,458 for the removed CSV encoding shape (2.65x); the self-provisioned local PostgreSQL TCP test copied 524,288 rows in 290,528,709 ns: 1,804,599 binary rows/s versus 594,945 CSV rows/s (3.03x).
- Parquet `arrow_ipc_to_parquet`: 134,219,889 physical bytes in 85,452,959 ns, or 1,497.9 MiB/s, including streaming encode, spill accounting, SHA-256, buffered durable write, and `sync_all`; equal-byte durable raw writes took 72,247,041 ns, or 1,771.7 MiB/s (0.845x).

The registered schema-ineligible fixtures were rejected during preflight: DuckDB `decimal256-v1`, PostgreSQL `unsupported-arrow-v1`, and Parquet `unsupported-arrow-v1`. No compatibility/scalar path was selected.

2026-07-18 addendum: after F2 made DuckDB's native resource envelope part of the selected path descriptor (`p3-f2-2026-07-14-v2`), the same DuckDB release benchmark was rerun for the generated envelope join. It appended 1,048,576 TLC-shaped rows in 106,608,709 ns: 9,835,744 rows/s, with direct Arrow appender throughput 9,816,297 rows/s and scalar control throughput 815,313 rows/s (12.06x). The stored machine report now joins DuckDB cells on the current descriptor evidence version and this rerun rate.

2026-07-18 addendum: after D8 moved Parquet to staged durable segment ingress (`p3-d8-2026-07-15-v5`), the local Parquet write-roofline benchmark was rerun for the generated envelope join. It wrote 134,244,673 physical bytes in 88,580,750 ns: 1,445.3 MiB/s, versus 1,414.8 MiB/s raw durable write in the same benchmark (1.022x). The stored machine report now joins Parquet cells on the current descriptor evidence version and this rerun rate. PostgreSQL remains the original D5 observation.

## Procedure

```text
CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-duckdb --release arrow_appender_tlc_envelope_benchmark --locked -- --ignored --nocapture
CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-postgres --release binary_copy_encoder_is_at_least_twice_csv --locked -- --ignored --nocapture
CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-postgres --release live_binary_copy_is_at_least_twice_csv --locked -- --ignored --nocapture
CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-parquet --release local_streaming_parquet_reaches_sixty_percent_of_write_roofline --locked -- --ignored --nocapture
```

F2 DuckDB descriptor refresh:

```text
CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-duckdb arrow_appender_tlc_envelope_benchmark --release --locked -j 12 -- --ignored --nocapture
```

D8 Parquet descriptor refresh:

```text
CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-parquet local_streaming_parquet_reaches_sixty_percent_of_write_roofline --release --locked -j 12 -- --ignored --nocapture
```

The PostgreSQL live benchmark initialized and stopped an isolated temporary cluster. The Parquet benchmark initially measured 1,142.7 MiB/s versus 1,991.5 MiB/s (0.574x), falsifying the gate. Inspection found unbuffered small writes beneath ArrowWriter. A 1 MiB accounted `BufWriter` coalesced those writes without changing identity or durability; independent reruns reached 0.895x and the retained exact sample above reached 0.845x.

## What it supports or challenges

This supports the D5 selected-path performance cells on one named host, the requirement that ineligible schemas remain explicit, and the claim that the selected adapter path—not a generic fallback—performs the mutation. It also challenges treating a previously green roofline as permanent evidence: host/filesystem variance exposed a real syscall-amplification defect that the durable benchmark caught.

## Limits

The measurements are one local host class. PostgreSQL is loopback rather than WAN; Parquet is local APFS rather than remote multipart; DuckDB is an adapter micro-path rather than the full TLC HTTPS run. D5 joins these exact observations only to their descriptor/evidence versions and does not generalize them to other hosts. Remote overlap remains owned by P3 G.
