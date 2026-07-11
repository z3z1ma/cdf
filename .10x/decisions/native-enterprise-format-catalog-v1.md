Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Native enterprise format catalog v1

## Context

The user expanded P3 from optimizing the current five file formats to native enterprise coverage across the inputs a general data-integration system encounters. “Everything” needs a concrete closeable catalog while the format-driver boundary keeps future additions local.

## Decision

P3 native enterprise catalog v1 includes:

- columnar/batch: Parquet, Arrow IPC file, Arrow IPC stream, ORC;
- delimited/text: CSV/TSV/custom delimited, fixed-width records, NDJSON, streaming JSON document, streaming XML records;
- self-describing/schema-bearing binary: Avro object container, Avro single-object with explicit schema, MessagePack sequence/array, CBOR sequence/array;
- schema-bound binary: length-delimited Protobuf with explicit descriptor set/message;
- spreadsheets: XLSX, XLS, XLSB, and ODS read-only worksheets;
- byte transforms: none, gzip, zstd, bzip2, xz, LZ4 frame, Snappy framed, and Brotli;
- character transforms for text codecs: UTF-8/BOM, UTF-16LE/BE with explicit/BOM authority, Windows-1252, and ISO-8859-1;
- archives: ZIP and TAR, including composition with supported byte compression.

All implementations are native Rust in the ordinary process. No JVM, Python parser subprocess, shell utility, or destination-side reader is the semantic implementation. A native library dependency is acceptable only after WS-L comparison, supply-chain review, and codec-local isolation.

Iceberg, Delta, and Hudi are table/source/destination protocols, not byte codecs. Database files such as SQLite are source drivers, not format drivers. Image/media/PDF extraction is interpretation/OCR and not implied by file ingestion. Their exclusion prevents the codec catalog from absorbing unrelated source semantics.

Every catalog entry must support bounded discovery where meaningful, pinned schema or explicit schema for non-self-describing inputs, preview/run parity, physical provenance, malformed-input policy, local/remote byte sources, memory accounting/spill, deterministic decode units/order, and reference-decoder performance evidence. A codec without truthful splitting/pushdown declares unsupported capabilities rather than faking them.

The native catalog is not the end of format extensibility. After v1, a new format is admitted by a focused codec ticket and registry/catalog entry without changing generic compiler/runtime code. Format requests are classified as codec, byte transform, archive container, or source/table protocol before implementation.

## Alternatives considered

- Optimize only current formats: rejected because it would preserve a narrow MVP architecture.
- Claim arbitrary formats via Python/fsspec: rejected because native performance, evidence, dependency isolation, and failure semantics would be lost.
- Treat every filename as a codec: rejected because archives, table formats, databases, and document interpretation have different lifecycle/state contracts.
- Promise literally every historical/proprietary format: rejected as non-falsifiable; v1 covers the enterprise families and leaves a local addition path.

## Consequences

WS-B expands into codec/transform/container children and cannot close on Parquet/CSV/JSON alone. New dependencies are not pre-approved. Performance targets for new codecs are ratios to their best viable native reference on the same host, with absolute numbers reported but not invented before the lab runs.
