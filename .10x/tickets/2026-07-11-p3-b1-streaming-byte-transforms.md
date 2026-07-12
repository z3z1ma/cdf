Status: open
Created: 2026-07-11
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md

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

## Progress and notes

- 2026-07-12: Replaced the unusable transform execution signature with explicit allocation and expansion authority. Every `ByteTransformDriver` now receives `ByteTransformRequest`: transform-class memory coordinator/consumer, preferred output chunk bound, expanded-byte and ratio ceilings no greater than its descriptor, optional planned input size, and cancellation. Invalid ownership, zero/oversized chunks, weakened ceilings, zero input identity, and ratio overflow fail before decode. No legacy signature or shim remains. This unblocks correct reserve-before-allocate gzip/zstd drivers; B1 remains open for implementations, integration, fuzzing, and envelope evidence. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-transform-allocation-authority.md`, `.10x/reviews/2026-07-12-p3-b1-transform-allocation-authority-review.md`.
- 2026-07-12: Added dependency-isolated `cdf-transform-gzip`, the first production `ByteTransformDriver`. It parses gzip framing incrementally across arbitrary input chunk boundaries, handles concatenated members, verifies header/payload/size checksums, enforces cancellation plus expanded-byte and ratio ceilings, and reserves both its native/header working set and every output chunk before allocation. The one-byte-input conformance test proves bounded overlap and full lease release. No full-object compatibility decoder exists in the new crate. B1 remains open for product-registry composition with the checksum publication barrier, removal of the old `cdf-formats` gzip paths, the remaining transforms, fuzzing, and envelope measurements. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-native-gzip-driver.md`, `.10x/reviews/2026-07-12-p3-b1-native-gzip-driver-review.md`.
- 2026-07-12: Removed two pieces of would-be per-codec plumbing from gzip before adding another transform. Runtime-neutral `AccountedByteCursor` now owns lease-safe incremental traversal and releases exhausted input before polling its successor; `TransformExpansionGuard` owns overflow-safe absolute/ratio accounting with bounded streaming grace and exact window/terminal enforcement. Gzip consumes both primitives, leaving framing and checksum logic as its only local concerns. Evidence: `.10x/evidence/2026-07-12-p3-b1-shared-transform-streaming-primitives.md`.
- 2026-07-12: Added dependency-isolated `cdf-transform-zstd` on the same neutral cursor/guard seam. It streams arbitrary rechunking, reinitializes across concatenated frames, verifies checksummed frames through zstd's native decoder, rejects truncation/corruption, and accounts output plus a conservative native working-set lease. The decoder caps frame windows at 64 MiB and reserves 68 MiB including context overhead, so concurrency backpressures through the ledger rather than hiding native allocations. B1 remains open for exact per-frame window admission, composition/legacy deletion, remaining transforms, fuzzing, and benchmarks. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-native-zstd-driver.md`, `.10x/reviews/2026-07-12-p3-b1-native-zstd-driver-review.md`.
- 2026-07-12: Added the neutral `TransformedByteSource` composition adapter and made maximum output chunk size an explicit transform descriptor capability. The adapter derives generation-bound transformed identity, preserves reopenability, deliberately removes seek/range/known-length claims, validates upstream input and expansion authorities, and composes any transform into the same sequential `ByteSource` contract consumed by format drivers. Exact ranges fail with the spool-adapter remediation instead of pretending compressed offsets map to output. Gzip/zstd and runtime composition tests remain green. Product registry selection remains open. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-transformed-byte-source.md`, `.10x/reviews/2026-07-12-p3-b1-transformed-byte-source-review.md`.
- 2026-07-12: Replaced zstd's fixed 68 MiB per-partition native reservation with frame-header-driven admission. The driver incrementally parses standard/skippable frame headers across arbitrary chunks, calculates the declared window, reserves only window + 4 MiB context, releases at each frame boundary, and rejects reserved bits or windows above 64 MiB before native decode. The concatenated/skippable one-byte fixture peak fell from the former ~71.3 MiB bound to 4,718,624 bytes (~15x lower) while corruption/truncation/expansion/cancellation laws remained green. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-zstd-frame-window-admission.md`, `.10x/reviews/2026-07-12-p3-b1-zstd-frame-window-admission-review.md`.
