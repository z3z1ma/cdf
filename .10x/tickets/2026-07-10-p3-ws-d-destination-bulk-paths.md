Status: open
Created: 2026-07-10
Updated: 2026-07-13
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md, .10x/tickets/2026-07-11-p0-destination-extension-boundary.md, .10x/tickets/done/2026-07-11-p3-a1-staged-ingress-final-binding.md, .10x/tickets/done/2026-07-07-duckdb-arrow-bulk-load-triage.md, .10x/tickets/done/2026-07-07-native-parquet-streaming-write-triage.md

# P3 WS-D: destination bulk paths

## Scope

Implement Arrow-native DuckDB append with truthful type fallback, Arrow-to-binary Postgres COPY with compatibility fallback, and streaming Parquet row-group/object-store multipart writes. Express materialization and bulk capabilities in destination sheets and preserve idempotency, merge/dedup, receipts, rollback, and verification.

Split by destination; no shared generic branch may name a concrete destination.

## Activated children

- `.10x/tickets/done/2026-07-11-p3-d1-bulk-path-contract.md` — done
- `.10x/tickets/done/2026-07-11-p3-d2-duckdb-arrow-bulk.md`
- `.10x/tickets/done/2026-07-11-p3-d3-postgres-binary-copy.md`
- `.10x/tickets/done/2026-07-11-p3-d4-parquet-streaming-writer.md`
- `.10x/tickets/done/2026-07-11-p3-d5-bulk-path-matrix.md`
- `.10x/tickets/2026-07-14-p3-d7-persistent-staged-ingress-stream.md`

## Acceptance criteria

- DuckDB, Postgres, and Parquet meet their ratified envelope rows with before/after evidence.
- Destination conformance re-proves append/replace/merge, duplicate replay, receipt verification, and rollback under bulk paths.
- Destinations needing full-package knowledge declare it rather than forcing the common path to materialize.

## Blockers

The D1-D5 implementation and matrix are terminal. D7 reopens the production wide-string ingress envelope after profiling falsified D2's per-segment appender lifetime. Parent closure also waits on reconciliation of its historical triage dependencies and program-level accounting.

## References

- `.10x/decisions/schema-planned-destination-bulk-paths.md`
- `.10x/specs/destination-bulk-path-runtime.md`
