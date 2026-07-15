Status: active
Created: 2026-07-10
Updated: 2026-07-15
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
- `.10x/tickets/done/2026-07-14-p3-d8-parquet-staged-parallel-ingress.md`
- `.10x/tickets/2026-07-15-p3-d9-content-reachability-authority.md`

## Acceptance criteria

- DuckDB, Postgres, and Parquet meet their ratified envelope rows with before/after evidence.
- Destination conformance re-proves append/replace/merge, duplicate replay, receipt verification, and rollback under bulk paths.
- Destinations needing full-package knowledge declare it rather than forcing the common path to materialize.

## Blockers

D8 is complete. D9 remains active shaping for the generic immutable-content claim/root/reclamation authority; WS-D stays open until that destination-neutral long-horizon storage guarantee closes.

## Evidence

- D1 records the neutral capability and bounded writer contract; D2-D4 record the Arrow-native DuckDB, binary Postgres COPY, and streaming Parquet paths; D5 owns the cross-destination guarantee/envelope matrix; D6 proves compact provenance equivalence.
- D7 removes the superseded per-segment staged API, retains one bounded native writer through a generic acknowledgement stream, preserves transaction/receipt/checkpoint semantics, reports destination ingress separately, reaches 9.51M TLC rows/s, and measures the exact 2.205 GB FineWeb package path at 2.017 seconds.
- D7's public Arrow-vtab alternative measured 2.845 seconds and uses upstream process-global retention. Its deletion is the measured no-action outcome required by P3's boundedness and no-legacy guardrails.
- 2026-07-15: Reopened after C4 measured 33.069 seconds in Parquet finalized-package ingress despite the isolated writer running at 0.786 of its raw-write roofline. D8 owns enrollment in generic staged ingress and deletion of the superseded finalized path.
- 2026-07-15: D8 closed after the current staged path reduced the complete 8.59 GB FineWeb command from 40.67 to 18.36 seconds, reached 0.779x the favorable same-data reference, bounded RSS at 1.463 GB, eliminated final-copy and legacy finalized ingress, and proved exact logical receipt/manifest identity at jobs 1/2/auto/4. D9 explicitly owns shared immutable-content reachability and keeps this parent open.

## Review

2026-07-14 closure review passed against the then-known evidence. C4 subsequently falsified the Parquet full-path performance premise, so that verdict no longer supports terminal status. Fresh review is required after D8.

## Retrospective

Destination throughput cannot be inferred from a broad package phase once staging overlaps construction. Giving staged ingress one typed lifetime and its own telemetry exposed the native writer as the actual cost, enabled a fair same-package strategy comparison, and avoided both a destination-specific orchestration branch and an unbounded Arrow registry. Future destinations implement one declared ingress category and inherit this lifecycle without runtime edits.

## References

- `.10x/decisions/schema-planned-destination-bulk-paths.md`
- `.10x/specs/destination-bulk-path-runtime.md`
