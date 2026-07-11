Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Native enterprise format catalog

## Purpose and scope

This specification defines catalog-v1 format semantics and cross-codec acceptance. Driver/runtime behavior is governed by `.10x/specs/native-format-codec-runtime.md`.

## Cross-codec laws

All catalog codecs MUST decode to the closed Arrow type system without text round-trips for typed binary/columnar values. Physical source type/framing/logical-type metadata MUST remain provenance. Unsupported or lossy mappings fail plan unless the existing explicit allowance governs them.

Discovery MUST pin deterministic schema evidence. Non-self-describing codecs require explicit schema/layout or bounded discovery; sampling is allowed under the active coverage contract and runtime nonconformance becomes residual/quarantine/error rather than silent schema mutation.

Every codec MUST stream or operate through bounded decode units, account parser/native buffers, reject adversarial expansion/depth/record sizes cleanly, and produce no accepted partial fatal window. Preview and run use the same options, detection, transforms, driver, reconciliation, and normalization.

All text codecs accept a pinned character transform. Auto mode recognizes UTF BOMs and otherwise uses UTF-8; invalid UTF-8 without an explicit alternate encoding fails with byte offset/remediation. Character transforms stream and cannot replace invalid sequences silently unless a separately compiled lossy policy exists.

Each codec records canonical options in the plan. Auto-detected dialect/framing/sheet/record root discovered during `cdf add` or discovery is pinned; runtime does not continuously re-guess. Limits include maximum record/cell/depth/expanded bytes and are plan evidence.

## Columnar and Arrow

Parquet MUST plan footer/row-group metadata through ranges, prune projection/predicates/pages where supported, decode row groups concurrently, preserve logical/physical types, and order rows by file then row group then row. Embedded Parquet compression is codec-internal; wrapping byte compression composes only through an explicit spool/seek plan.

Arrow IPC file MUST support local or remote range/seek input and bounded verified spool fallback. Arrow IPC stream is sequential and schema-first. Dictionary batches, continuation markers, compression, and record-batch order are preserved; decoder microbatches do not redefine canonical package segments.

ORC MUST plan stripes from immutable footer/postscript metadata, push projection/predicates truthfully, decode stripes concurrently, preserve ORC logical/physical provenance, and order by stripe/row. Unsupported ORC type mappings fail at reconciliation.

## Delimited and fixed-width text

Delimited options include delimiter, quote, double-quote/escape, header rows, comment prefix, line ending, trim policy, null tokens, and ragged-row policy. TSV is a pinned delimited preset. Dialect detection is bounded suggestion/pinning. Parallel byte-range decode is legal only when quote-aware boundary scanning proves independent record starts; otherwise sequential streaming is required.

Fixed-width requires a versioned layout of named character or byte ranges, encoding, line ending, trim/null policy, and optional record discriminator. Character ranges apply after decoding; byte ranges apply before decoding and cannot split a multibyte character. Overlap, gaps marked required, short/long records, and discriminator mismatch follow explicit error/quarantine policy.

## JSON, MessagePack, and CBOR

NDJSON emits one JSON value per line. JSON document supports one record, a top-level array, or a pinned streaming record selector expressible without whole-document DOM. Arbitrary filter/script selectors are excluded from the codec and belong in transforms. Depth, token, string, and record byte limits are enforced.

MessagePack and CBOR support a top-level array or concatenated/sequence framing pinned in options. Integer width/sign, binary versus string, floats, timestamps/tags/extensions, maps, and nested structures map to Arrow with physical provenance. Unknown extension/tag handling is explicit: preserve as typed binary residual, quarantine, or fail; never stringify implicitly.

## Avro and Protobuf

Avro object-container files use embedded writer schema, block framing, sync markers, and supported native block codecs. Decode units are blocks. Reader schema is the pinned constraint and Avro resolution is compiled into ordinary reconciliation evidence. Avro single-object requires explicit writer schema/fingerprint authority; no ambient registry lookup is inferred.

Avro nullable `[null,T]` maps to nullable `T`; general unions map to Arrow dense union with branch identity metadata. Logical types map losslessly where Arrow supports them; unsupported logical types retain physical provenance and follow reconciliation policy.

Length-delimited Protobuf requires an explicit `FileDescriptorSet`, fully qualified message name, and framing/maximum message size. Field numbers, presence, oneof branch, enum numeric/name authority, maps, repeated fields, well-known types, and unknown raw fields are preserved in metadata/residual evidence. Unknown fields default to binary residual capture under evolve and fail under freeze; they are never dropped silently. Protobuf streams without deterministic framing fail plan.

## XML

XML uses streaming parsing with an explicit namespace-aware absolute record path or a bounded discovered suggestion that is pinned. Mapping options cover attributes, element text, repeated children, mixed content policy, and namespace naming. DTD, external entities, XInclude, and network resolution are always disabled. Depth, entity/text, and record limits prevent expansion attacks. Arbitrary XPath predicates requiring DOM/global context are excluded.

## Spreadsheets

XLSX, XLS, XLSB, and ODS are read-only workbook codecs. A worksheet or sheet glob is required/pinned; each selected sheet is a logical file child/unit ordered by workbook sheet order. Options pin header row/range, data range, empty-row termination, merged-cell policy, and formula policy.

CDF MUST never execute formulas, macros, links, or external data connections. Formula cells use cached results only when present and record formula provenance; absent cached results quarantine/fail by policy. Date/time conversion records workbook epoch/date system. Merged cells default to top-left value plus null elsewhere; fill-down requires explicit configuration. Hidden sheets/rows/columns are included by default and may be explicitly excluded with plan evidence.

## Byte compression and archives

Gzip, zstd, bzip2, xz, LZ4 frame, Snappy framed, and Brotli MUST stream, verify checksums where the format provides them, handle concatenated members according to pinned codec rules, and enforce expansion/ratio/window limits. Raw unframed Snappy/LZ4 requires explicit framing metadata and is not auto-detected.

ZIP and TAR are container drivers. Members become deterministic logical child partitions with archive identity plus normalized member path, member size/CRC/checksum where available, and ordinal. Member globbing, nested archive depth, member count, per-member/total expanded bytes, compression ratio, duplicate names, symlink/hardlink policy, and path traversal are planned limits. No member path is written outside controlled scratch. ZIP central-directory access uses ranges when possible; TAR streams in archive order. Archive completion advances the outer file manifest only after all selected members complete.

## Performance acceptance

Existing P3 envelope rows remain mandatory. Each new codec/transform MUST achieve at least 0.6x the selected same-host native reference throughput for equivalent semantic work unless a focused evidence record ratifies a lower ceiling caused by unavoidable format structure; CDF evidence overhead remains separately measured. Parallel-capable codecs scale until CPU/device/network saturation. Sequential formats must show the parser—not framework orchestration—is the dominant CPU cost.

## Explicit exclusions

This catalog does not implement Iceberg/Delta/Hudi, database-file semantics, OCR/document extraction, formula/macro execution, ambient schema registries, lossy replacement decoding by default, or dynamic plugins.
