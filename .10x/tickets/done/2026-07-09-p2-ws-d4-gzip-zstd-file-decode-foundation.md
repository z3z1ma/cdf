Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-d-file-source-globs-manifest-compression.md
Depends-On: .10x/specs/data-onramp-file-sources-transports.md, .10x/tickets/done/2026-07-08-p2-ws-d1-file-glob-partition-planning.md, .10x/tickets/done/2026-07-09-p2-ws-d3-file-manifest-incremental-noop.md

# P2 WS-D4 gzip and zstd file decode foundation

## Scope

Implement the first transparent compression slice for local file resources: gzip and zstd compressed NDJSON inputs decode through the same file runtime path as uncompressed NDJSON, with explicit compression metadata preserved in file source evidence.

## Acceptance criteria

- Declarative file resources accept `compression = "auto" | "none" | "gzip" | "zstd"` for file formats where byte-stream decompression is meaningful.
- Auto mode detects gzip and zstd by extension and magic bytes for local files, with explicit `compression` overriding extension inference.
- Local `.ndjson.gz` and `.ndjson.zst` resources plan, preview, and run through the existing file partition/runtime path without manual preprocessing.
- Decompression is streaming or bounded to the existing file read boundary for this foundation slice; it must not write temporary decompressed files into the project.
- Source-position/package evidence preserves the compressed file identity and records compression mode in metadata where file-source evidence is already emitted.
- Mismatched explicit compression fails before emitting partial batches and names the file plus both signals.

## Evidence expectations

Focused `cdf-formats` and `cdf-declarative` tests for gzip/zstd read success and mismatch failures, preview/open parity tests for compressed local NDJSON, no temporary decompressed artifacts, and normal formatting/diff checks. CLI/conformance S3 coverage may remain a later WS-I child if this slice only lands the lower runtime foundation.

## Explicit exclusions

This ticket does not implement remote compressed reads, zip archive member semantics, compression for Parquet internals, cloud object stores, large-N coalescing, or final S3 conformance.

## Progress and notes

- 2026-07-09: Opened while H2 `cdf add` runs in parallel. Keep write scope away from top-level CLI command grammar to avoid conflicts with H2.
- 2026-07-09: Activated by Worker D4. Implementation is scoped to `cdf-formats` byte-stream decode, `cdf-declarative` local file compression planning/metadata, and focused compression tests; top-level CLI grammar remains untouched.
- 2026-07-09: Implemented local gzip/zstd NDJSON decode foundation. Declarative file resources now accept `compression = "auto" | "none" | "gzip" | "zstd"` with omitted compression defaulting to auto; local planning records compression metadata for relevant compressed or explicit resources; preview/open decode `.ndjson.gz` and `.ndjson.zst` through the existing file partition/runtime path; explicit mismatches fail during planning with file, declared mode, extension signal, and magic-byte signal. Evidence: `.10x/evidence/2026-07-09-p2-ws-d4-gzip-zstd-file-decode-foundation.md`. Review: `.10x/reviews/2026-07-09-p2-ws-d4-gzip-zstd-file-decode-foundation-review.md`.

## Blockers

None for local gzip/zstd NDJSON foundation.
