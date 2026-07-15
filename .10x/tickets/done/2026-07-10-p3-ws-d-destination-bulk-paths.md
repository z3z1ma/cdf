Status: done
Created: 2026-07-10
Updated: 2026-07-14
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
- `.10x/tickets/done/2026-07-14-p3-d7-persistent-staged-ingress-stream.md`

## Acceptance criteria

- DuckDB, Postgres, and Parquet meet their ratified envelope rows with before/after evidence.
- Destination conformance re-proves append/replace/merge, duplicate replay, receipt verification, and rollback under bulk paths.
- Destinations needing full-package knowledge declare it rather than forcing the common path to materialize.

## Blockers

None.

## Evidence

- D1 records the neutral capability and bounded writer contract; D2-D4 record the Arrow-native DuckDB, binary Postgres COPY, and streaming Parquet paths; D5 owns the cross-destination guarantee/envelope matrix; D6 proves compact provenance equivalence.
- D7 removes the superseded per-segment staged API, retains one bounded native writer through a generic acknowledgement stream, preserves transaction/receipt/checkpoint semantics, reports destination ingress separately, reaches 9.51M TLC rows/s, and measures the exact 2.205 GB FineWeb package path at 2.017 seconds.
- D7's public Arrow-vtab alternative measured 2.845 seconds and uses upstream process-global retention. Its deletion is the measured no-action outcome required by P3's boundedness and no-legacy guardrails.

## Review

2026-07-14 closure review reconciled every child and the parent criteria. All concrete destination behavior remains in its adapter behind one ingress capability; generic orchestration names no destination; append/replace/merge, replay, rollback, receipt, and provenance conformance are terminal in D2-D6; and the reopened wide-string regression is terminal in D7. Verdict: **pass**. Program-level end-to-end envelope reconciliation remains owned by Z1 rather than keeping this implementation workstream artificially open.

## Retrospective

Destination throughput cannot be inferred from a broad package phase once staging overlaps construction. Giving staged ingress one typed lifetime and its own telemetry exposed the native writer as the actual cost, enabled a fair same-package strategy comparison, and avoided both a destination-specific orchestration branch and an unbounded Arrow registry. Future destinations implement one declared ingress category and inherit this lifecycle without runtime edits.

## References

- `.10x/decisions/schema-planned-destination-bulk-paths.md`
- `.10x/specs/destination-bulk-path-runtime.md`
