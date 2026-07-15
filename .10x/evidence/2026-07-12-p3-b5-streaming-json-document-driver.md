Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b5-json-codecs.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Streaming JSON-document driver evidence

## What was observed

`cdf-format-json::JsonDocumentFormatDriver` now discovers and decodes a top-level object or array of objects through the neutral `FormatDriver`/`ByteSource` contracts. Its bounded state machine converts object records to an incremental Arrow JSON tape stream without materializing the input document. It preserves nested objects/arrays and delimiters inside strings, caps nesting at 256, rejects non-object records and malformed top-level framing, and charges framed output to the shared memory coordinator.

All five original file formats now resolve execution through `FormatRegistry`. The source-owned legacy `compile_format` tree and declared-schema decoder escape hatch were deleted. Project discovery likewise has one registered-format adapter rather than row-format-specific discovery functions.

## Procedure

- `cargo test -p cdf-format-json`
- `cargo test -p cdf-source-files local_json_document_discovers_and_streams_through_registered_driver`
- `cargo test -p cdf-project local_json_document_discovery_uses_the_registered_driver_manifest_path`
- `cargo clippy -p cdf-format-json -p cdf-runtime -p cdf-source-files -p cdf-project -p cdf-cli --all-targets -- -D warnings`
- Searched generic source/project/declarative code for the deleted `compile_format`, old declared-schema stream entry point, and closed first-party `FileFormat` variants; no matches remained.
- Inspected `cargo tree -p cdf-format-json --edges normal --depth 1`; the parser is codec-local and the leaf imports neutral CDF contracts, not source/project/CLI/transport crates.

## What this supports

Discovery and execution now share the same JSON driver interpretation. One-byte rechunking tests prove framing invariance; bounded sampling stops only after complete records and reports actual consumed source bytes; source execution preserves `FileManifest` and releases memory to zero.

## Limits

This does not close B5. NDJSON/JSON discovery still retains the bounded sample until Arrow schema inference completes; oversized-record admission, random property/fuzz coverage, row-local quarantine/residual evidence, REST page decode, selectors, and measured throughput remain outstanding. FX1 also still needs remote external-provider coverage and its full closure audit.
