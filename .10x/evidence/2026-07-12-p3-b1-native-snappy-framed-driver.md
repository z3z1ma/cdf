Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-12-p0-object-store-quick-xml-advisory.md

# Native Snappy framed transform

## What was observed

`cdf-transform-snappy` incrementally parses the Snappy framed format through `AccountedByteCursor`. It requires the canonical stream identifier, supports repeated identifiers and bounded skippable/padding chunks, rejects reserved and raw unframed chunks, decodes compressed blocks through `snap::raw::Decoder`, verifies every block's masked CRC32C before making its bytes available, and enforces absolute/ratio/cancellation authority.

One 160 KiB transform-class lease covers compressed and decoded 64 KiB frame buffers plus decoder state. Output slices receive separate leases. When the requested output chunk contains the verified block, the block `Vec` transfers directly into `Bytes`; smaller targets copy only the requested slice. The cursor's new bulk `read_exact`/`skip_exact` methods copy contiguous spans rather than polling every byte.

## Performance procedure and result

Command:

`cargo test --release -p cdf-transform-snappy --locked snappy_driver_reference_rate -- --ignored --nocapture`

Same-process 64 MiB deterministic fixture, same compressed bytes and semantic checksum work:

- Native `snap::read::FrameDecoder`: 31.895 ms.
- CDF accounted driver: 18.990 ms.
- Reference throughput ratio: 1.680x.

The first measured implementation used an async byte-at-a-time payload loop and achieved only 0.152x reference (30.599 ms versus 200.803 ms); it was rejected and deleted before commit. Bulk cursor traversal produced the accepted result.

## Correctness and dependency procedure

- `cargo test -p cdf-transform-snappy --locked`
  - Result: 3 correctness tests passed; release benchmark remained intentionally ignored in the normal tier.
  - Covered concatenated identifiers, skippable chunks, one-byte transport rechunking, checksum corruption, truncation, raw framing rejection, expansion ceiling, cancellation, peak/current memory.
- `cargo clippy -p cdf-runtime -p cdf-transform-snappy --all-targets --locked -- -D warnings`
  - Result: passed.
- `cargo deny --locked check`
  - Bans, licenses, and sources passed. Advisories failed on then-present `quick-xml 0.39.4` through `object_store 0.13.2` (RUSTSEC-2026-0195); `.10x/tickets/done/2026-07-12-p0-object-store-quick-xml-advisory.md` records its subsequent remediation.
- `cargo vet --locked --no-minimize-exemptions`
  - Full source/build/unsafe review for new `crc32c 0.6.8` was recorded as `safe-to-deploy`; it no longer appears in the unvetted set. Fourteen pre-existing dependencies remain unvetted outside this slice.

## What this supports or challenges

This supports B1's framed-Snappy correctness, constant-memory, dependency-isolation, and ≥0.6x reference-rate criteria. It also demonstrates the value of one shared accounted cursor: a measured framing bottleneck was fixed once below codecs.

## Limits

Product registry composition, fuzz-generated chunk corpora, the remaining transform catalog, and whole-pipeline compressed text measurements remain open under B1.
