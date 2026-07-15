Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Neutral transformed byte source

## What was observed

`cdf-runtime::TransformedByteSource` composes an arbitrary `ByteSource` and `ByteTransformDriver` behind the existing sequential byte-source interface. It validates the upstream chunk target and transform expansion ceilings once, passes the shared memory owner/consumer and cancellation into each open, and advertises only capabilities that remain true after a non-random transform.

Transformed identity hashes upstream stable identity/generation/checksum plus transform id and semantic version. It does not label that hash as an output content checksum; strong/content-addressed upstream identity becomes strong transformed generation identity, while weak identity remains weak. Output length is unknown. Seek, exact ranges, and range concurrency are removed. Reopenability is preserved. Exact-range calls fail with a bounded-spool remediation.

`ByteTransformDescriptor::maximum_output_chunk_bytes` now separates output chunk authority from total native working-set authority. Request validation enforces both, and gzip/zstd declare their real 16/32 MiB output caps.

## Procedure

- `cargo test -p cdf-runtime --locked transformed_byte_source`
  - Result: passed; covered identity/version sensitivity, capability reduction, sequential accounted composition, lease release, and exact-range rejection.
- `cargo test -p cdf-transform-gzip -p cdf-transform-zstd --locked`
  - Result: 6 tests passed after the descriptor contract change.
- `cargo clippy -p cdf-runtime -p cdf-transform-gzip -p cdf-transform-zstd --all-targets --locked -- -D warnings`
  - Result: passed.

## What this supports or challenges

This supports B1/FX1's architecture: transforms compose at one neutral byte-source seam instead of adding gzip/zstd branches to each format, source, transport, discovery, preview, and run path.

## Limits

The standard product registry and file compiler do not yet select this adapter. No seekable spool wrapper is included. Product integration, legacy deletion, publication atomicity, and performance evidence remain B1 work.
