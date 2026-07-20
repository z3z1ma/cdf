Status: done
Created: 2026-07-10
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md, .10x/tickets/done/2026-07-11-p0-destination-extension-boundary.md, .10x/tickets/done/2026-07-11-p3-a1-staged-ingress-final-binding.md, .10x/tickets/done/2026-07-07-duckdb-arrow-bulk-load-triage.md, .10x/tickets/done/2026-07-07-native-parquet-streaming-write-triage.md

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
- `.10x/tickets/done/2026-07-15-p3-d9-content-reachability-authority.md`
- `.10x/tickets/cancelled/2026-07-18-p3-d10-duckdb-stream-scan-staged-ingress.md`
- `.10x/tickets/done/2026-07-18-p3-d11-duckdb-arrow-ipc-handoff-falsification.md`
- `.10x/tickets/cancelled/2026-07-18-p3-d12-duckdb-arrow-ipc-handoff-ingress.md`
- `.10x/tickets/cancelled/2026-07-18-p3-d13-duckdb-parquet-handoff-ingress.md`
- `.10x/tickets/done/2026-07-18-p3-d14-duckdb-nanoarrow-080-lz4-revalidation.md`
- `.10x/tickets/done/2026-07-18-p3-d15-canonical-package-row-ordinal.md`
- `.10x/tickets/cancelled/2026-07-19-p3-d16-postgres-package-copy-amortization.md`

## Acceptance criteria

- DuckDB, Postgres, and Parquet meet their ratified envelope rows with before/after evidence.
- Destination conformance re-proves append/replace/merge, duplicate replay, receipt verification, and rollback under bulk paths.
- Destinations needing full-package knowledge declare it rather than forcing the common path to materialize.

## Blockers

None. All activated children are terminal. D16 retained the faster direct-target Postgres append/replace path but closed cancelled because its measured `1.346x` comparable indexed-append improvement did not meet the ticket's 2x stretch criterion; that stretch miss does not reopen the already-green binary COPY envelope or leave legacy staging in place.

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
- 2026-07-18: D12 failed its own product-retention threshold and was cancelled. Full-CDF EC2 local TLC measured `38.624s` for opt-in Arrow IPC `INSERT ... read_arrow`, `37.274s` for CTAS with full nullability restoration, `37.205s` for CTAS with row-key-only nullability restoration, and a `179s` timeout with `CDF_DUCKDB_THREADS=16`; all are worse than the appender baseline. The prototype code was removed before commit. D13 was opened for the tuned Parquet handoff fallback because its generated same-host evidence was the only then-retained materialization branch near the `8–12s` roofline; D13 was later cancelled by full-CDF product evidence.
- 2026-07-18: D13 failed its own product-retention threshold and was cancelled. Full-CDF EC2 local TLC measured `37.546s` for the opt-in Parquet handoff CTAS variant and `38.828s` for the simplified planned-table + `INSERT ... read_parquet(...)` variant; both are worse than the appender baseline despite local semantic tests passing. The prototype code, Parquet dependency, runtime capability, and tests were removed before commit.
- 2026-07-18: D14 corrected D11's artifact-specific LZ4 conclusion. A pinned DuckDB nanoarrow extension built against Apache nanoarrow `0.8.0` with `NANOARROW_IPC_WITH_LZ4=ON` directly read all 215 current canonical CDF full-year TLC segments and materialized 41,169,720 rows in median `4.558788174s`, `9,030,847` rows/s, with `2.289 GiB` peak process RSS and no cgroup pressure or spill event. This no-duplicate-handoff path is now the retained DuckDB product candidate; D12 and D13 remain cancelled because their separate duplicate handoffs failed full-CDF evidence.
- 2026-07-19: D14 and D15 closed on the leaner stock-library architecture. The sole DuckDB product path uses a destination-local public-C-API parallel table function over canonical LZ4 IPC segments; the custom DuckDB runtime, nanoarrow extension lifecycle, appender, bridges, feature branches, and unused dependencies are deleted. The clean full-product three-sample median is `10.255642670s` / `4,014,348` rows/s for 41,169,720 rows at the default budget, versus the old appender's approximately `34s`. Canonical `_cdf_package_row_ord` supplies destination-neutral row order to DuckDB and Postgres while Parquet strips it from visible payloads.
- 2026-07-19: Opened D16 from honest cross-destination macro evidence. Postgres remains correct and its direct binary COPY control remains `3.33x` CSV, but the full package path pays 215 segment-scoped COPY/publication cycles and reaches only `400,862` rows/s. D16 owns amortizing that lifecycle without text fallback, full-package materialization, generic runtime branches, or regenerated provenance.
- 2026-07-19: D16 closed with an honest partial-performance outcome. One package-wide COPY alone was neutral because append still staged and rewrote the payload; deleting append/replace staging reduced the comparable indexed-append cell from `103.399s` to `76.809s` (`1.346x`) and a corrected standalone fresh-target cell completed in `61.216s` / `672,531` rows/s with exact rows and one provenance index. The direct binary-vs-CSV control remains `3.33x`, but the comparable full-product result is not 2x, so D16 is cancelled rather than falsely marked done. The faster path remains because it deletes a redundant full-table write without weakening transaction, receipt, provenance, or merge semantics.
- 2026-07-19: D9 closed the final lifecycle gap with a destination-neutral immutable-content claim/root/reclamation authority. Parquet enrolls once per output object and root; bounded SQLite indexes and durable reservations prevent full-store scans and deletion races, exact local generation checks retain replaced data, and remote providers without conditional delete retain safely. The release Parquet writer measured 1,567.9 MiB/s, 0.919x raw sequential write, preserving the workstream's performance floor.

## Review

Verdict: pass. The reopened findings are terminal: D8 removed Parquet final-copy amplification and superseded ingress; D14/D15 replaced DuckDB's row appender with one stock-library canonical-segment scanner and standardized package row order; D16 deleted redundant Postgres append/replace staging; D9 supplied generic immutable-content lifetime authority without a destination-specific heartbeat or runtime branch. Destination behavior remains behind ingress/capability boundaries. Focused conformance, receipt/replay tests, EC2 macro evidence, and release roofline evidence cover the three adapters. No active child, legacy product path, or critical/high review finding remains.

## Retrospective

Destination throughput cannot be inferred from a broad package phase once staging overlaps construction. Giving staged ingress one typed lifetime and its own telemetry exposed the native writer as the actual cost, enabled a fair same-package strategy comparison, and avoided both a destination-specific orchestration branch and an unbounded Arrow registry. Future destinations implement one declared ingress category and inherit this lifecycle without runtime edits.

## References

- `.10x/decisions/schema-planned-destination-bulk-paths.md`
- `.10x/specs/destination-bulk-path-runtime.md`
