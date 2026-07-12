Status: open
Created: 2026-07-11
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/2026-07-07-rest-json-to-arrow-performance-triage.md

# P3 B5: tape-based NDJSON, JSON, and REST page decode

## Scope

Replace production DOM/full-byte JSON paths with streamed tape/SIMD-class physical Arrow decoding for NDJSON, JSON document selectors, and REST pages on the CPU executor; retain bounded DOM only for discovery flexibility.

## Acceptance criteria

- Compressed/uncompressed NDJSON and JSON do not materialize full inputs/documents.
- REST I/O overlaps CPU decode without blocking the I/O executor.
- Fatal windows publish no partial accepted batch; recoverable record errors preserve exact quarantine/residual evidence.
- Depth/token/record limits, selector framing, random rechunking, and jobs are deterministic.
- JSON meets the 3x-current and aggregate envelope targets with dependency evidence.

## Evidence expectations

arrow-json/simd candidate comparison, dependency gate, malformed/fuzz corpus, selector/REST parity, compressed profiles, memory, and reference goldens.

## Explicit exclusions

No XML/MessagePack/CBOR parsing.

## Blockers

Depends on transforms, FX1, L5, and absorbs the REST triage.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`

## Progress and notes

- 2026-07-12: Added `cdf-format-json::NdjsonFormatDriver` using Arrow JSON's incremental tape decoder directly over accounted `ByteSource` chunks. Discovery is bounded and no-copy across retained source chunks; decode is incremental with pre-admitted output leases and 64k/byte targets. Product composition, local discovery, and gzip object-store discover/pin/run are green; the old source NDJSON fallback is fail-closed. The ticket remains open for JSON documents, malformed/fuzz/oversized-row memory proof, row-local quarantine parity, and the 3x/300–500 MB/s envelope.
