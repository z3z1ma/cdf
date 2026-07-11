Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Format extension and streaming audit

## Question

What must change so adding a native file format is localized, discovery and execution share one implementation, remote/local inputs compose identically, and decoding is bounded/parallel without bloating every crate's build graph?

## Sources and methods

Inspected `cdf-formats`, declarative file declarations/compiler/runtime, file transport, REST JSON decoding, Cargo dependency edges, active extension invariant, P2 transport/discovery contracts, and P3 decoder/runtime workstreams. Traced format selection, detection, discovery, schema reconciliation, compression, local/remote opening, batch collection, and source positions.

## Findings

Format identity is duplicated as closed enums in declarative configuration and `cdf-formats`. Adding one format requires match edits across declaration schema, compiler inference/discovery, file runtime, magic/extension confirmation, compression exclusions, and codec reader dispatch. This is shared-orchestration proliferation, not a localized adapter.

`FormatRead` owns `Vec<Batch>`. Arrow IPC readers collect every batch; Parquet readers collect; CSV/JSON/NDJSON begin from complete byte slices; gzip/zstd local reads decompress to one `Vec<u8>`. JSON document discovery and runtime materialize documents. Remote non-Parquet formats spool the full object, then invoke local-path code. Remote Arrow IPC is rejected even though its seekability need is a transport/capability question.

Discovery and execution are separate function families with format-specific matches. Declared-schema behavior also varies inside codec functions, so codecs both decode physical data and partially own reconciliation semantics. This recreates the P2 “two schema truths” risk at each new format.

Transport returns owned `Vec<u8>` ranges and complete `Vec` listings behind a synchronous mutex/private runtime. A codec cannot request an accounted sequential stream, parallel ranges, or seek/spool capability through one neutral interface. Compression is another closed enum intertwined with formats instead of a composable byte-transform layer.

The `cdf-formats` crate links Arrow CSV/JSON/IPC, Parquet, gzip, and zstd together. An edit or consumer of one codec inherits the whole format implementation graph. Enterprise additions such as Avro/ORC/XML/spreadsheets would make this worse.

Parquet row groups and other independent blocks are subunits of one logical file checkpoint. They may execute concurrently and retry independently, but `FileManifest` advancement must occur only after every required unit for that file succeeds. Treating row groups as unrelated files would corrupt incrementality.

## Conclusion

Create runtime-neutral `ByteSource`, `FormatDriver`, `ByteTransformDriver`, and registry contracts. Replace configuration enums with validated `FormatId`/driver options. Drivers own detection, bounded discovery, decode-unit planning, and physical Arrow decoding; the shared graph owns schema reconciliation, contracts, normalization, package evidence, and checkpoint meaning.

Split first-party codec implementations into dependency-isolated crates and compose them explicitly at the product root. Logical files contain deterministically planned decode units; format parallelism never changes file-level state completion or package order.

## Limits

This audit does not select every future parser dependency or implement a stable dynamic ABI. The native format catalog and each codec require focused dependency/performance evidence after the neutral boundary exists.
