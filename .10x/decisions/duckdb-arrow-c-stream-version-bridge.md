Status: active
Created: 2026-07-11
Updated: 2026-07-11

# DuckDB Arrow C Stream version bridge

## Context

CDF's governed Arrow/DataFusion tuple uses arrow-rs 59.1.0. The pinned `duckdb` 1.10504.0 binding, including its current upstream `main` at `1265c995e00ab37141d0d57f0eea841037581697`, uses arrow-rs 58. Its `appender-arrow` API therefore cannot accept CDF `RecordBatch` values as Rust types even though both versions implement the stable Arrow C Stream ABI.

The binding's Arrow table-function helper is not acceptable for a long-running integration runtime: its source explicitly retains every passed `RecordBatch` in a process-global arena that is never freed. IPC serialization would avoid `unsafe` but would add an encode/decode copy to the hottest destination path and compromise the P3 envelope. Scalar rows are the measured baseline problem.

## Decision

CDF will enable the pinned DuckDB binding's `appender-arrow` feature and cross the arrow-rs 59/58 boundary through one isolated Arrow C Stream ownership transfer. The bridge MAY contain one documented `unsafe` pointer read after compile-time size/alignment assertions over the two `#[repr(C)]` `FFI_ArrowArrayStream` structs. Ownership moves exactly once: the Arrow 59 wrapper is held in `ManuallyDrop`; the Arrow 58 stream reader becomes the sole owner and invokes the Arrow 59 release callback.

The bridge MUST:

- remain private to `cdf-dest-duckdb` and expose only a safe `RecordBatch` conversion function;
- pin the DuckDB dependency tuple and fail compilation if either ABI wrapper's size or alignment diverges;
- preserve buffers zero-copy through C Stream callbacks, never transmute Arrow Rust arrays or schemas directly;
- reject missing, duplicate, or extra batches and propagate C Stream errors;
- have property/adversarial coverage over batch sizes, null patterns, and the supported DuckDB type matrix;
- stay out of generic runtime and destination contracts.

The `vtab-arrow` query-parameter path is forbidden until upstream removes its permanent process-global retention. A future DuckDB binding that consumes CDF's Arrow major directly SHOULD delete the bridge.

## Alternatives considered

- Keep scalar append: rejected because it materializes every cell and cannot meet D2.
- Serialize Arrow IPC between versions: rejected as a hot-path copy and CPU tax.
- Use the Arrow vtab plus `INSERT SELECT`: rejected because the pinned binding permanently retains every registered batch.
- Downgrade CDF's Arrow/DataFusion tuple: rejected because one destination cannot govern the whole execution engine.
- Fork or reproduce DuckDB's data-chunk FFI: rejected because it would duplicate a large unsafe type-conversion implementation and expand the maintenance/safety surface.
- Wait for dependency alignment: rejected because current upstream still uses Arrow 58 and D2 has an ABI-standard solution now.

## Consequences

DuckDB receives native vectorized batches without row scalarization or unbounded vtab retention. The only new unsafe surface is a small, testable interoperability bridge based on Arrow's stable C ABI. Two Arrow Rust majors remain in the build graph until upstream aligns; this was already true through the DuckDB dependency. The bridge and its tests become part of the pinned dependency upgrade gate.
