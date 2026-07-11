Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/2026-07-11-p3-a2-unified-memory-ledger.md

# P3 B1: streaming byte and character transforms

## Scope

Implement codec-registry transforms for gzip, zstd, bzip2, xz, LZ4 frame, Snappy framed, Brotli, and the catalog character encodings; migrate full-buffer gzip/zstd paths; add expansion/window/checksum limits and measured native implementations.

## Acceptance criteria

- Every transform streams accounted chunks and never buffers the expanded object.
- Concatenated members, checksum failure, truncation, raw/framed ambiguity, BOM/explicit encoding conflict, invalid text, expansion ratio, and cancellation follow the catalog spec.
- Existing gzip/zstd package outputs remain semantically identical across local/remote inputs.
- Each transform meets its reference ratio or records a focused architectural ceiling.

## Evidence expectations

Dependency reviews, malformed/bomb fuzz corpus, memory/RSS, local/remote composition, checksum/encoding goldens, and before/after throughput/profile evidence.

## Explicit exclusions

No archive member enumeration or format parsing.

## Blockers

Depends on L5, FX1, and the memory ledger.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`
