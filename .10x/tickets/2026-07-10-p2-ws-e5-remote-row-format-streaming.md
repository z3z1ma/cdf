Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-08-p2-ws-e-remote-transports.md
Depends-On: .10x/tickets/done/2026-07-10-p2-ws-e3-cloud-object-stores-and-http-templates.md

# P2 WS-E5 — Remote row-format streaming and bounded spool

## Scope

Enable CSV, JSON, and NDJSON over HTTP/object stores with transparent gzip/zstd handling through a budgeted streaming/spool boundary that composes with the file transport and preserves per-file evidence.

## Acceptance criteria

- Remote CSV/JSON/NDJSON and `.gz`/`.zst` inputs execute through the same partition semantics as local inputs.
- Compressed bytes are streamed; no decompressed whole-file `Vec<u8>` is introduced.
- Any seek-required staging has an explicit disk budget and fails cleanly before exhaustion.
- Preview and run share the exact path; source positions retain remote identity.
- S3 compressed NDJSON conformance fixture passes.

## Explicit exclusions

Zip archives, whose member-manifest semantics remain separately deferred by decision.

## Evidence expectations

Large compressed fixture RSS/budget test, preview/run parity, source-position evidence, clippy, and review.

## Blockers

None.

## Progress and notes

- 2026-07-10: Added transport-level gzip/zstd confirmation, range-streamed bounded spooling for remote CSV/JSON/NDJSON, exact remote `FileManifest` position restoration, and an explicit configurable 64 GiB default disk ceiling.
- 2026-07-10: Added bounded row-format discovery shared across local and remote CSV/JSON/NDJSON: 4,096 records and at most 8 MiB of source bytes, with gzip/zstd decoded incrementally during discovery and complete manifest evidence.
- 2026-07-10: Replaced nested `futures_executor` use around `object_store` with a dedicated transport-owned Tokio I/O runtime, proving object-store reads from within the existing async resource-open path.
- 2026-07-10: S3-compatible `.ndjson.gz` fixture now discovers, pins, executes 10,000 records, and preserves its remote position. Remaining acceptance work is eliminating the downstream `FormatRead` whole-input/materialized-batch behavior; that must land through P3's channel runtime rather than a second decoder API.
