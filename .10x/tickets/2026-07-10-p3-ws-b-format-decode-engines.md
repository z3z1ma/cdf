Status: open
Created: 2026-07-10
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/2026-07-07-rest-json-to-arrow-performance-triage.md

# P3 WS-B: format decode engines

## Scope

Make each source format stream into Arrow efficiently: row-group-parallel Parquet with pushdown, chunk-parallel CSV where safe, tape-based JSON/NDJSON, streamed gzip/zstd windows, and REST CPU-pool page decode. Preserve fail-closed decoding, P2 schema reconciliation, residual capture, and deterministic partition output.

Split by codec and shared byte-transform seam before implementation. Every codec implements `.10x/specs/native-format-codec-runtime.md`; no new format may extend the current enum/match architecture. Any parser/decompression dependency is research/dependency-gate work, not an assumed addition.

The native closeout catalog is `.10x/specs/native-enterprise-format-catalog.md`; Parquet/Arrow/delimited/JSON optimization alone does not close this workstream.

## Activated children

- `.10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md`
- `.10x/tickets/2026-07-11-p3-b2-parquet-codec.md`
- `.10x/tickets/2026-07-11-p3-b3-arrow-ipc-codecs.md`
- `.10x/tickets/2026-07-11-p3-b4-delimited-fixed-width-codecs.md`
- `.10x/tickets/2026-07-11-p3-b5-json-codecs.md`
- `.10x/tickets/2026-07-11-p3-b6-avro-codecs.md`
- `.10x/tickets/2026-07-11-p3-b7-orc-codec.md`
- `.10x/tickets/2026-07-11-p3-b8-xml-codec.md`
- `.10x/tickets/2026-07-11-p3-b9-spreadsheet-codecs.md`
- `.10x/tickets/2026-07-11-p3-b10-protobuf-codec.md`
- `.10x/tickets/2026-07-11-p3-b11-messagepack-cbor-codecs.md`
- `.10x/tickets/2026-07-11-p3-b12-archive-containers.md`
- `.10x/tickets/2026-07-11-p3-b13-native-format-matrix.md`

## Acceptance criteria

- No production row-format path requires full decompressed input or whole-page DOM materialization except bounded discovery samples.
- Parquet projection/predicate pushdown and deterministic row-group concurrency meet the envelope.
- Malformed-input property/fuzz tests prove no partial accepted batch escapes.
- Every changed engine has same-harness before/after evidence.

## Blockers

Blocked until WS-L baseline evidence and the FX1 format-driver boundary exist.
