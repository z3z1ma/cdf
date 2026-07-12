Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Native Brotli driver

## What was observed

`cdf-transform-brotli` drives Rust Brotli's incremental state machine directly over `AccountedByteCursor`; it does not adapt a blocking `Read` or retain the expanded object. The driver uses strict standard-window decoding (large-window extension rejected), admits a 32 MiB decoder/window/table working-set lease before constructing native state, supports concatenated streams, and enforces cancellation plus absolute/ratio expansion authority on every output chunk.

Brotli has no framing magic or checksum. Detection is therefore extension/explicit-selection only and corruption is reported by the decoder. The descriptor's 10,000:1 maximum is a capability ceiling rather than a product default; the absolute expanded-byte ceiling remains authoritative and a compiled request may select a lower ratio. This avoids rejecting legitimate highly compressible Brotli payloads merely because Brotli materially out-compresses the other codecs.

## Procedure and results

- `cargo test -p cdf-transform-brotli --locked`: passed (2 correctness tests; one release benchmark ignored).
- Concatenated streams decoded from one-byte upstream chunks; all managed leases released and peak managed bytes remained within the 32 MiB working lease plus output/input chunks.
- Corruption, truncation, expanded-byte ceiling, and pre-cancelled execution fail closed.
- `cargo clippy -p cdf-transform-brotli --all-targets --locked -- -D warnings`: passed.
- `cargo test --release -p cdf-transform-brotli --locked brotli_driver_reference_rate -- --ignored --nocapture`: `brotli_reference_ms=20.594`, `brotli_driver_ms=21.303`, ratio `0.967x`; passed the `>=0.6x` reference floor.
- Brotli 8.0.4 was already present in the pinned Parquet graph; this leaf adds no new third-party package.

## What this supports

Brotli can be a bounded, parser-local transform with throughput essentially equal to its native synchronous reader while preserving the neutral memory/cancellation/expansion contract.

## Limits

The managed-memory test proves CDF lease behavior, not exact allocator RSS inside Rust Brotli. B1's stress/RSS layer must falsify the 32 MiB conservative native reservation. Registry composition and product-default expansion policy remain open.

