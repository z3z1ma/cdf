Status: open
Created: 2026-07-10
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/2026-07-10-p3-ws-l-performance-lab.md, .10x/tickets/2026-07-11-p0-destination-extension-boundary.md, .10x/tickets/2026-07-07-duckdb-arrow-bulk-load-triage.md, .10x/tickets/2026-07-07-native-parquet-streaming-write-triage.md

# P3 WS-D: destination bulk paths

## Scope

Implement Arrow-native DuckDB append with truthful type fallback, Arrow-to-binary Postgres COPY with compatibility fallback, and streaming Parquet row-group/object-store multipart writes. Express materialization and bulk capabilities in destination sheets and preserve idempotency, merge/dedup, receipts, rollback, and verification.

Split by destination; no shared generic branch may name a concrete destination.

## Acceptance criteria

- DuckDB, Postgres, and Parquet meet their ratified envelope rows with before/after evidence.
- Destination conformance re-proves append/replace/merge, duplicate replay, receipt verification, and rollback under bulk paths.
- Destinations needing full-package knowledge declare it rather than forcing the common path to materialize.

## Blockers

Blocked until WS-L baseline evidence exists; streaming Parquet integration also depends on the WS-A session shape.
