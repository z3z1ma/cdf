Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Native LZ4 frame driver

## What was observed

`cdf-transform-lz4` implements the neutral `ByteTransformDriver` contract without importing a source, project, transport, destination, or sibling-codec crate. It parses standard LZ4 frames directly over `AccountedByteCursor`, supports independent and linked blocks plus concatenated/skippable frames, verifies header/block/content XXH32 checksums, enforces content size, rejects legacy/raw/dictionary framing, and admits a header-derived block working set before allocation.

The driver retains at most the selected frame block input, one decoded block, the 64 KiB linked dictionary, one requested output chunk, and one upstream chunk. Expanded bytes are never accumulated across blocks.

## Procedure and results

- `cargo test -p cdf-transform-lz4 --locked`: passed (3 correctness tests; one release benchmark ignored).
- The one-byte-input linked/concatenated test decoded 380,000 bytes across arbitrary boundaries, released all leases, and stayed below the declared working-set bound plus one output/input chunk.
- Corrupt content checksum, truncation, raw framing, expansion ceiling, and cancellation tests fail closed.
- `cargo clippy -p cdf-transform-lz4 --all-targets --locked -- -D warnings`: passed.
- `cargo test --release -p cdf-transform-lz4 --locked lz4_driver_reference_rate -- --ignored --nocapture`: `lz4_reference_ms=20.491`, `lz4_driver_ms=25.711`, ratio `0.797x`; passed the `>=0.6x` reference floor.
- `cargo deny --locked check`: all four gates passed.
- `cargo vet --locked --no-minimize-exemptions`: succeeded. LZ4 Flex and xxhash were already present in the locked, policy-covered graph; this slice added no third-party package.

## What this supports

LZ4 framing can be added as a parser-local leaf through the shared transform seam with bounded memory and competitive native throughput. Linked-block history does not require full-object retention.

## Limits

The leaf stream exposes verified blocks before an optional whole-frame content checksum is observed. Standard product composition must apply the already-required checksum publication barrier before accepted downstream visibility. Registry wiring, fuzz corpus integration, and removal of superseded product paths remain in B1/FX1.
