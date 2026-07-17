Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/done/2026-07-11-p3-g1-streaming-transport-byte-sources.md, .10x/tickets/done/2026-07-11-p3-g2-range-readahead-spool-controller.md, .10x/tickets/done/2026-07-11-p3-g3-codec-download-decode-overlap.md

# Streaming transform product composition

## What was observed

Production file execution now composes an injected local, HTTP, or object-store `ByteSource` with the registry-selected `ByteTransformDriver` before joining the registered format driver's declared access policy. Sequential codecs consume the transformed accounted stream directly. Adaptive codecs consume one transformed-output spool whose unknown final length grows shared spill authority before each disk write. The previous provider-spool followed by transform-spool path is bypassed whenever an injected byte source exists.

The object-store gzip-NDJSON fixture completes with a one-byte disk-spool ceiling, produces both rows with the original remote `FileManifest` position, and records zero current and peak spill bytes. The external format-plus-transform fixture now proves a genuinely sequential third-party driver can compose without runtime dispatch edits. Gzip-Parquet still passes through one transformed-output spool, as required until a seekable/splittable transform or growing-spool reader exists.

## Procedure

- `cargo fmt --all`
- `cargo test -p cdf-source-files --lib`: 26 passed, 0 failed.
- `cargo clippy -p cdf-source-files --all-targets -- -D warnings`: passed.
- `git diff --check`: passed.

## What this supports

- Remote compressed row inputs can overlap download, decompression, and decode without materializing or spilling the expanded object.
- Transform selection and format access policy remain registry-driven; adding a transport, transform, or sequential format requires no source-runtime match branch.
- Unknown transformed lengths can be admitted to disk without writing bytes outside the shared spill ledger.
- The transform driver's expansion ceiling is independent of disk-spool configuration on direct streaming paths; disk-spool bounds continue to govern materialized compatibility/adaptive paths.

## Limits

Weak HTTP providers still use the compatibility spool because they cannot prove one generation during an independently reopened stream. Compressed binary discovery still uses a bounded private spool. Adaptive transformed decode does not yet read a growing spool concurrently, and this record does not supply throughput, cancellation-chaos, live-cloud, or malformed-transform fuzz evidence.
