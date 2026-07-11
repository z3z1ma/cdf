Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/2026-07-11-p0-sx1-source-extension-boundary.md, .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md, .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md, .10x/specs/data-onramp-file-sources-transports.md

# P3 WS-G: remote I/O overlap

## Scope

Build on the P2 transport facade with parallel ranged Parquet GETs, bounded readahead, download/decode overlap, connection pooling, HTTP/2 where supported, and local sequential readahead. Keep logical-file identity, egress, credentials, retries, and manifest semantics unchanged.

## Acceptance criteria

- Remote TLC ingest is network-bound with decode hidden where host/network permit.
- Ranged reads and readahead are ledger-owned and cancellation-safe.
- Transport-specific tuning remains behind the transport facade and capability data.
- The full-year TLC envelope target has a labeled profile and replayable recorded-fixture test.

## Blockers

Blocked on WS-A pipeline, WS-B Parquet engine, and WS-L baseline.
