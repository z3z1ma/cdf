Status: active
Created: 2026-07-10
Updated: 2026-07-18
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
- `.10x/tickets/cancelled/2026-07-18-p3-d10-duckdb-stream-scan-staged-ingress.md`
- `.10x/tickets/done/2026-07-18-p3-d11-duckdb-arrow-ipc-handoff-falsification.md`
- `.10x/tickets/2026-07-18-p3-d12-duckdb-arrow-ipc-handoff-ingress.md`

## Acceptance criteria

- DuckDB, Postgres, and Parquet meet their ratified envelope rows with before/after evidence.
- Destination conformance re-proves append/replace/merge, duplicate replay, receipt verification, and rollback under bulk paths.
- Destinations needing full-package knowledge declare it rather than forcing the common path to materialize.

## Blockers

D8 is complete. D9 remains active shaping for the generic immutable-content claim/root/reclamation authority. D10 is cancelled as a measured no-go: the attempted DuckDB stream-scan staged-ingress product path timed out under EC2 promotion evidence and is not exposed by runtime capabilities. D11 is done: uncompressed DuckDB Arrow IPC handoff is retained as the next implementation candidate, while current-segment direct read and LZ4 handoff are killed for the pinned nanoarrow build. D12 owns the opt-in production path. WS-D stays open until the destination-neutral long-horizon storage guarantee and the DuckDB envelope-critical materialization gap close under a retained design.

## Evidence

- D1 records the neutral capability and bounded writer contract; D2-D4 record the Arrow-native DuckDB, binary Postgres COPY, and streaming Parquet paths; D5 owns the cross-destination guarantee/envelope matrix; D6 proves compact provenance equivalence.
- D7 removes the superseded per-segment staged API, retains one bounded native writer through a generic acknowledgement stream, preserves transaction/receipt/checkpoint semantics, reports destination ingress separately, reaches 9.51M TLC rows/s, and measures the exact 2.205 GB FineWeb package path at 2.017 seconds.
- D7's public Arrow-vtab alternative measured 2.845 seconds and uses upstream process-global retention. Its deletion is the measured no-action outcome required by P3's boundedness and no-legacy guardrails.
- 2026-07-15: Reopened after C4 measured 33.069 seconds in Parquet finalized-package ingress despite the isolated writer running at 0.786 of its raw-write roofline. D8 owns enrollment in generic staged ingress and deletion of the superseded finalized path.
- 2026-07-15: D8 closed after the current staged path reduced the complete 8.59 GB FineWeb command from 40.67 to 18.36 seconds, reached 0.779x the favorable same-data reference, bounded RSS at 1.463 GB, eliminated final-copy and legacy finalized ingress, and proved exact logical receipt/manifest identity at jobs 1/2/auto/4. D9 explicitly owns shared immutable-content reachability and keeps this parent open.
- 2026-07-18: G4 EC2 evidence reopened DuckDB materialization as the dominant envelope owner. Lab-only DuckDB Arrow stream-scan with the existing `_cdf_row_key` column and bounded `threads=16`/`1GiB` DuckDB resource settings materialized 41.2M TLC-shaped rows in median `5.111650191s`, while current CDF local TLC-to-DuckDB remains `33.955522533s`. D10 owns turning that destination-crate-only signal into a retained staged-ingress path or recording a measured no-go.
- 2026-07-18: D10 recorded the measured no-go. The production stream-scan staged-ingress attempt timed out at `119000ms` on full-year local TLC, the CTAS variant also timed out, and a one-partition smoke timed out at `59000ms`; after the rejection patch, DuckDB runtime capabilities again advertise only the measured appender path. G4 remains the active performance owner for the remaining DuckDB/package materialization envelope gap.
- 2026-07-18: Removed D10's disabled product stream-scan remnants from `cdf-dest-duckdb` rather than leaving legacy fallback code: the crate now has one staged writer shape matching its advertised appender-only capability. `cargo fmt --all && cargo fmt --check && CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-duckdb --locked -j 12` passed after the deletion.
- 2026-07-18: Opened and reshaped D11 after G4 tuned the DuckDB Parquet handoff diagnostic from the rejected 64 MiB policy (`20.615s`, ~7.22 GiB RSS) to a credible 128–256 MiB row-group policy (`~10.47s`, ~2.75 GiB RSS), then received the Arrow IPC/nanoarrow hypothesis. D11 closed with a precise split: nanoarrow `read_arrow(...)` works for uncompressed Arrow IPC and materialized 41,169,720 TLC-shaped rows in median `9.308415940s`; it rejects LZ4 IPC and therefore cannot directly read current CDF package segments. D12 owns a DuckDB-destination-owned, opt-in, uncompressed IPC handoff implementation; tuned Parquet handoff remains the fallback if D12 fails product promotion evidence. The generic runtime must remain destination-neutral.

## Review

2026-07-14 closure review passed against the then-known evidence. C4 subsequently falsified the Parquet full-path performance premise, so that verdict no longer supports terminal status. Fresh review is required after D8.

## Retrospective

Destination throughput cannot be inferred from a broad package phase once staging overlaps construction. Giving staged ingress one typed lifetime and its own telemetry exposed the native writer as the actual cost, enabled a fair same-package strategy comparison, and avoided both a destination-specific orchestration branch and an unbounded Arrow registry. Future destinations implement one declared ingress category and inherit this lifecycle without runtime edits.

## References

- `.10x/decisions/schema-planned-destination-bulk-paths.md`
- `.10x/specs/destination-bulk-path-runtime.md`
