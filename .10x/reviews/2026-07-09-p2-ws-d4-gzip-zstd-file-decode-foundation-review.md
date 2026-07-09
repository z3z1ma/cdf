Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-d4-gzip-zstd-file-decode-foundation.md
Verdict: pass

# P2 WS-D4 gzip and zstd file decode foundation review

## Target

Review of the local gzip/zstd NDJSON decode foundation implemented for `.10x/tickets/done/2026-07-09-p2-ws-d4-gzip-zstd-file-decode-foundation.md`.

## Assumptions tested

- Omitted `compression` defaults to `auto` so compressed NDJSON can be transparent without changing existing uncompressed resources.
- Auto detection must use local file extension and magic bytes, and conflicts must fail before opening batches.
- Explicit compression may override extension inference only when magic bytes confirm the explicit mode.
- Source evidence must preserve compressed file identity rather than reporting decompressed temporary paths.
- The ticket allows bounded decode at the existing byte-reader boundary for this foundation slice.

## Findings

No blocking findings.

Minor residual: `cdf-formats` still feeds Arrow JSON/CSV readers from in-memory bytes, so gzip/zstd readers stream into the existing byte-oriented boundary instead of providing an end-to-end streaming JSON decoder. This is acceptable for D4 because the ticket permits "streaming or bounded to the existing file read boundary" and the implementation does not write temporary decompressed files.

Minor residual: compression metadata is carried in declarative partition metadata, while the kernel `FilePosition` shape remains path, size, ETag, and SHA-256 only. This is acceptable for D4 because the ticket's write scope excludes kernel type changes and compressed file identity is preserved in `FileManifest`.

## Verdict

Pass. The implementation is scoped to local gzip/zstd byte-stream NDJSON, keeps remote/zip/Parquet internals out of scope, validates mismatches before batch emission, preserves compressed file identity, and has focused passing evidence in `.10x/evidence/2026-07-09-p2-ws-d4-gzip-zstd-file-decode-foundation.md`.

## Residual risk

Remote compressed reads, zip member semantics, final S3 conformance, cloud object stores, and deeper end-to-end streaming reader work remain outside this ticket and stay owned by the active P2 parent graph.
