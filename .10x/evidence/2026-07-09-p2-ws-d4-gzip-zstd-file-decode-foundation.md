Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-d4-gzip-zstd-file-decode-foundation.md, .10x/specs/data-onramp-file-sources-transports.md

# P2 WS-D4 gzip and zstd file decode foundation evidence

## What was observed

Local gzip and zstd NDJSON decode support was implemented in `cdf-formats` and wired through `cdf-declarative` file partition planning/opening. Focused compression tests pass for the format reader and declarative runtime path. Formatting and whitespace checks pass.

The first focused test attempt failed because the workspace filesystem had only about 330 MB free and Cargo could not create build artifacts under `target/`. No Rust test assertion failed in that attempt. After confirming no active Cargo/rustc process and observing `target/debug/incremental` was about 58 GB, the generated incremental cache was removed and tests were rerun with `CARGO_INCREMENTAL=0`.

## Procedure

- `cargo fmt --all`
  - Result: passed.
- `cargo test -p cdf-formats compression --locked` and `cargo test -p cdf-declarative compression --locked`
  - Result: both initially failed during compilation with `No space left on device`.
- `df -h . /tmp`
  - Result before cleanup: root filesystem was at 100% with about 330 MB available.
- `du -sh target`
  - Result: `target` was about 638 GB.
- `du -sh target/debug/incremental target/debug/build`
  - Result: `target/debug/incremental` was about 58 GB; `target/debug/build` was about 33 GB.
- `rm -rf target/debug/incremental && df -h .`
  - Result: freed space; root filesystem had about 53 GB available.
- `CARGO_INCREMENTAL=0 cargo test -p cdf-formats compression --locked`
  - Result: passed; 1 test passed, 0 failed.
- `CARGO_INCREMENTAL=0 cargo test -p cdf-declarative compression --locked`
  - Result: passed; 3 tests passed, 0 failed.
- `cargo fmt --all -- --check`
  - Result: passed.
- `git diff --check`
  - Result: passed.

## What this supports

- `cdf-formats` can decode gzip and zstd compressed NDJSON file sources while preserving `SourcePosition::FileManifest` identity for the compressed file path, compressed byte size, and compressed checksum.
- Declarative file resources accept `compression = "auto" | "none" | "gzip" | "zstd"` and default omitted compression to `auto`.
- Local auto mode plans and opens `.ndjson.gz` and `.ndjson.zst` through the existing file partition/runtime path.
- Explicit gzip can override misleading extension inference when magic bytes confirm gzip.
- Explicit gzip/zstd mismatch fails during local partition planning before any batches are opened and names the file, declared mode, extension signal, and magic-byte signal.
- No temporary decompressed sibling files are created by the tested gzip/zstd NDJSON path.
- Workspace formatting and whitespace checks pass after the D4 change.

## Limits

This evidence is scoped to local byte-stream compressed NDJSON foundation behavior. It does not cover remote compressed reads, zip archive members, Parquet byte-stream compression, cloud object stores, large-N coalescing, or final S3 conformance. The implementation decodes through streaming gzip/zstd readers into the existing byte-oriented JSON/NDJSON reader boundary rather than replacing that boundary with an end-to-end streaming Arrow JSON reader.
