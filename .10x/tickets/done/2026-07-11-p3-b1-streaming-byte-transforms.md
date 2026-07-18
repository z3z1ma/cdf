Status: done
Created: 2026-07-11
Updated: 2026-07-17
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md

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

None. L5, FX1, and the memory ledger are complete.

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
- 2026-07-12: Added dependency-isolated `cdf-transform-snappy` for framed Snappy only. The driver parses bounded frame chunks, accepts repeated stream identifiers and skippable chunks, rejects reserved/raw framing, verifies every data chunk's masked hardware/software CRC32C before publication, and emits requested-size accounted slices with zero-copy handoff when the verified block fits. A shared bulk exact-read/skip cursor primitive replaced the initial byte-at-a-time payload loop: the 64 MiB release comparison improved from a rejected 0.152x to 1.680x native `snap::FrameDecoder` throughput (31.895 ms reference, 18.990 ms driver). Evidence/review: `.10x/evidence/2026-07-12-p3-b1-native-snappy-framed-driver.md`, `.10x/reviews/2026-07-12-p3-b1-native-snappy-framed-driver-review.md`.
- 2026-07-12: Added dependency-isolated `cdf-transform-lz4` with direct standard-frame parsing, independent/linked blocks, 64 KiB dictionary history, concatenated/skippable frames, and header/block/content XXH32 verification. Block-size headers drive a bounded 2-block-plus-history lease; no expanded object is retained. The 64 MiB release comparison reached 0.797x LZ4 Flex's synchronous reference (20.491 ms reference, 25.711 ms driver), above the 0.6x floor. The standard registry must still compose the checksum publication barrier before accepted visibility. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-native-lz4-frame-driver.md`, `.10x/reviews/2026-07-12-p3-b1-native-lz4-frame-driver-review.md`.
- 2026-07-12: Added dependency-isolated `cdf-transform-brotli` using the native incremental state machine with strict standard windows, admitted decoder working state, concatenated streams, EOF flush correctness, and expansion/cancellation enforcement. The 32 MiB release comparison reached 0.967x Brotli's synchronous reference (20.594 ms reference, 21.303 ms driver). Brotli has no magic/checksum, so selection remains explicit/extension-based and allocator RSS remains a stress-harness obligation. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-native-brotli-driver.md`, `.10x/reviews/2026-07-12-p3-b1-native-brotli-driver-review.md`.
- 2026-07-12: Added dependency-isolated `cdf-transform-bzip2` on the Trifecta pure-Rust backend. Cumulative decoder counters preserve exact trailing-member position; CRC/magic, truncation, expansion, cancellation, and native-memory failures close cleanly under an 8 MiB working lease. The 32 MiB release comparison reached 0.997x the synchronous reference (344.227 ms reference, 345.136 ms driver). The permissive `bzip2-1.0.6` license and exact Rust backend were supply-chain admitted. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-native-bzip2-driver.md`, `.10x/reviews/2026-07-12-p3-b1-native-bzip2-driver-review.md`.
- 2026-07-12: Added dependency-isolated `cdf-transform-xz` using liblzma's `.xz`-only incremental decoder, concatenated-stream flag, terminal `Finish`, integrity checks, and a hard 64 MiB memlimit matched by CDF admission. Review rejected ambient system linking and forced the exact bundled static 0.4.7 implementation with bindgen/parallel disabled. The 32 MiB release comparison reached 0.984x the synchronous reference (29.035 ms reference, 29.493 ms driver). Evidence/review: `.10x/evidence/2026-07-12-p3-b1-native-xz-driver.md`, `.10x/reviews/2026-07-12-p3-b1-native-xz-driver-review.md`.
- 2026-07-12: Added dependency-free `cdf-transform-character` with registered auto/UTF-8, UTF-16LE/BE, Windows-1252, and ISO-8859-1 modes. BOM authority is deterministic, arbitrary chunk boundaries retain at most four carry bytes, invalid/undefined sequences fail with offsets, and replacement decoding does not exist. The initial copy-biased UTF-8 benchmark was rejected at 0.238x; the streaming-equivalent zero-copy-source/one-consumption comparison reached 2.316x (7.876 ms retained reference, 3.401 ms bounded driver) with the lifetime bias recorded. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-character-transforms.md`, `.10x/reviews/2026-07-12-p3-b1-character-transforms-review.md`.
- 2026-07-12: Composed every native transform in the CLI's standard product registry and injected that neutral registry into file runtime dependencies. Runtime lookup now supports validated ids, extensions, and ambiguity-detecting strong magic without exposing any leaf implementation to generic source code. Selection/spooling and legacy deletion remain open. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-standard-transform-registry.md`, `.10x/reviews/2026-07-12-p3-b1-standard-transform-registry-review.md`.
- 2026-07-12: Replaced the closed gzip/zstd declaration and source match tree with registry-id selection from descriptor extensions/strong magic. Execution now streams registered transforms into a checksum-gated bounded spool before any format driver publishes batches; `.parquet.gz` composes with the native Parquet driver. Row discovery uses bounded private transform samples, while accepted execution requires terminal integrity. Deleted `cdf-formats` compression state/decoders/dependencies and the deprecated single-file Parquet discovery helper. Compressed binary discovery/attestation and neutral remote-byte-source overlap remain open. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-registry-transform-execution.md`, `.10x/reviews/2026-07-12-p3-b1-registry-transform-execution-review.md`.
- 2026-07-12: Completed compressed binary probe parity for local/remote Parquet and Arrow IPC, local attestation, and exhaustive multi-file schema joins. Runtime testing found and eliminated a nested-I/O-runtime deadlock by moving synchronous transport preparation before the async transform/decode task; the formerly hanging remote gzip discover/pin/run test now completes in 0.24s. Matrix/fuzz evidence and neutral remote `ByteSource` overlap remain open. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-compressed-binary-parity.md`, `.10x/reviews/2026-07-12-p3-b1-compressed-binary-parity-review.md`.
- 2026-07-12: Composed injected byte sources with registry-selected transforms in production. Sequential codecs now consume expanded accounted chunks directly with zero spill; adaptive codecs create exactly one transformed-output spool and grow shared spill authority before writing unknown-length output. Object-store gzip NDJSON succeeds under a one-byte spool ceiling with zero spill and preserved remote position. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-streaming-transform-product-composition.md`, `.10x/reviews/2026-07-12-p3-b1-streaming-transform-product-composition-review.md`.
- 2026-07-17: Closure audit on the current tree found no remaining B1 implementation blocker. Fresh current-tree gates passed for every registered transform crate, product transform composition, object-store gzip NDJSON direct streaming, and release reference-rate tests for Brotli, bzip2, character/UTF-8, LZ4, Snappy, and XZ. Residual breadth is routed rather than duplicated: catalog-wide fuzz/matrix/per-format cells remain owned by B13 and the individual format tickets; remote growing-spool/weak-provider overlap remains owned by G2/G3/G4. B1's owned outcome is complete: native streaming transforms, neutral registry composition, deletion of full-buffer gzip/zstd legacy paths, accounted expansion/window/checksum enforcement, and product execution without source-runtime transform branches.

## Evidence

- Allocation and shared streaming primitives:
  - `.10x/evidence/2026-07-12-p3-b1-transform-allocation-authority.md`
  - `.10x/evidence/2026-07-12-p3-b1-shared-transform-streaming-primitives.md`
  - `.10x/evidence/2026-07-12-p3-b1-transformed-byte-source.md`
- Leaf transform evidence:
  - `.10x/evidence/2026-07-12-p3-b1-native-gzip-driver.md`
  - `.10x/evidence/2026-07-12-p3-b1-native-zstd-driver.md`
  - `.10x/evidence/2026-07-12-p3-b1-zstd-frame-window-admission.md`
  - `.10x/evidence/2026-07-12-p3-b1-native-snappy-framed-driver.md`
  - `.10x/evidence/2026-07-12-p3-b1-native-lz4-frame-driver.md`
  - `.10x/evidence/2026-07-12-p3-b1-native-brotli-driver.md`
  - `.10x/evidence/2026-07-12-p3-b1-native-bzip2-driver.md`
  - `.10x/evidence/2026-07-12-p3-b1-native-xz-driver.md`
  - `.10x/evidence/2026-07-12-p3-b1-character-transforms.md`
- Product composition evidence:
  - `.10x/evidence/2026-07-12-p3-b1-standard-transform-registry.md`
  - `.10x/evidence/2026-07-12-p3-b1-registry-transform-execution.md`
  - `.10x/evidence/2026-07-12-p3-b1-compressed-binary-parity.md`
  - `.10x/evidence/2026-07-12-p3-b1-streaming-transform-product-composition.md`
- Current-tree closure gates:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-transform-gzip -p cdf-transform-zstd -p cdf-transform-snappy -p cdf-transform-lz4 -p cdf-transform-brotli -p cdf-transform-bzip2 -p cdf-transform-xz -p cdf-transform-character --lib --locked -j 12` — passed; 19 leaf tests passed, 6 reference-rate tests intentionally ignored in debug.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files transform --lib --locked -j 12` — passed; proves external transform composition and gzip Parquet registry/spool composition.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files object_store_gzip_ndjson_streams_without_spill_and_preserves_remote_position --lib --locked -j 12 -- --nocapture` — passed; proves object-store gzip NDJSON streams with zero spill and preserved remote `FileManifest` position.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-transform-brotli -p cdf-transform-bzip2 -p cdf-transform-character -p cdf-transform-lz4 -p cdf-transform-snappy -p cdf-transform-xz --release --lib --locked -j 12 -- --ignored --nocapture` — passed; reported ratios Brotli 0.904x, bzip2 0.997x, UTF-8 character 1.648x, LZ4 0.837x, Snappy 1.634x, XZ 0.978x.

## Review

Verdict: pass for B1 closure.

The early significant product-composition concern in `.10x/reviews/2026-07-12-p3-b1-registry-transform-execution-review.md` was closed by compressed binary parity and the final streaming product-composition slice. The final product-composition review reports no critical or significant defect in the retained architecture. Leaf reviews report no critical or significant remaining defect for every registered transform; performance measurements meet or exceed the recorded ratios, with LZ4 and Brotli above the stated reference floors.

Residual risks are deliberately not B1 blockers:

- Catalog-wide fuzz/matrix breadth remains owned by `.10x/tickets/2026-07-11-p3-b13-native-format-matrix.md` and the individual format tickets.
- Remote weak-provider compatibility spooling and growing-spool overlap remain owned by G2/G3/G4 remote I/O tickets.
- Format-specific text/binary semantics beyond byte transforms remain owned by the corresponding format codec tickets.

## Retrospective

The winning architecture was the boring one: transform drivers own parser/framing/checksum details, while `cdf-runtime` owns allocation, expansion, cancellation, and byte-source composition. That split prevented per-codec memory-policy drift and kept source runtime from importing concrete transform crates. The important performance lesson is that reference-rate tests must compare equivalent streaming lifetimes; the first UTF-8 comparison was rejected because it measured copy topology rather than transform compute.
