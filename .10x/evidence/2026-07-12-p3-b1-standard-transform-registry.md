Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Standard byte-transform registry composition

## What was observed

The CLI composition root now registers every first-party byte transform—gzip, zstd, Snappy framed, LZ4 frame, Brotli, bzip2, xz, and all catalog character encodings—into one `ByteTransformRegistry`. `FileRuntimeDependencies` receives that registry beside the format registry; `cdf-source-files` imports no first-party transform crate.

The neutral registry resolves by validated id, extension, and strong magic and rejects runtime detection ambiguity. Its descriptors remain deterministically ordered by id.

## Procedure

- `cargo test -p cdf-runtime byte_transform_registry_resolves_names_extensions_and_magic`
- `CARGO_BUILD_JOBS=1 cargo check -p cdf-source-files`
- `CARGO_BUILD_JOBS=1 cargo check -p cdf-cli`
- `cargo fmt --all -- --check`

All commands passed. A broad parallel metadata check was abandoned after independent `rustc` jobs slept at 0% CPU for minutes; serial scoped checks completed without diagnostics and avoided treating a redundant whole-graph build as evidence.

## What this supports

Product composition has one implementation-aware edit point. Generic source/runtime code depends only on the neutral registry contract, so subsequent selection and streaming-spool work can be registry-driven without adding codec match branches.

## Limits

This slice injects and queries the registry but does not yet replace the closed gzip/zstd declaration or legacy decoder dispatch. Those remain owned by P3 B1 and P0 FX1.
