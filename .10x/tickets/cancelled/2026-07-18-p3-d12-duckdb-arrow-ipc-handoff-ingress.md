Status: cancelled
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-18-p3-d11-duckdb-arrow-ipc-handoff-falsification.md, .10x/tickets/2026-07-11-p3-g4-tlc-remote-io-envelope.md, .10x/tickets/2026-07-15-p3-d9-content-reachability-authority.md

# P3 D12: DuckDB Arrow IPC handoff ingress

## Scope

Implement an opt-in DuckDB destination-owned bulk ingress path that writes validated/normalized batches, including `_cdf_row_key`, to temporary uncompressed Arrow IPC handoff files and materializes the target table through DuckDB community `nanoarrow` `read_arrow(list_of_files)`. The path must live behind DuckDB's destination ingress implementation and existing generic staged-ingress lifecycle; orchestration branches on declared destination capability only, never on DuckDB identity, Arrow IPC filenames, or handoff internals.

The implementation may initially be selected by an explicit documented knob so the current appender default cannot regress. Default promotion requires same-host full-CDF EC2 evidence that beats the appender baseline and does not violate memory/disk budgets.

## Non-goals

- No package artifact format change; CDF package segments remain their current identity-bearing compressed Arrow IPC artifacts.
- No direct DuckDB read of current package segments; D11 killed that path for the pinned nanoarrow build.
- No LZ4 Arrow IPC handoff until the pinned DuckDB/nanoarrow build supports it or a separately ratified extension/dependency path proves it.
- No stream-scan/`duckdb_arrow_scan` callback revival.
- No generic runtime branch naming DuckDB, Arrow IPC, Parquet, or handoff paths.
- No hard-coded performance cap without a knob.
- No unbounded handoff disk growth; temporary bytes must be ledger-accounted or fail cleanly against the configured spill/disk budget.
- No default promotion without EC2 evidence.

## Acceptance Criteria

- DuckDB's destination sheet/runtime capability exposes the Arrow IPC handoff path as a destination-owned bulk ingress strategy without changing the common runtime's destination-neutral contract.
- The opt-in path installs/loads the pinned community `nanoarrow` extension through the DuckDB destination boundary and produces a typed error with actionable remediation if the extension is unavailable.
- Handoff files are uncompressed Arrow IPC files with `_cdf_row_key` already materialized, deterministic file boundaries, deterministic row-key ranges, and durable cleanup semantics tied to the destination staging/lease authority.
- Handoff disk use is accounted as explicit temporary spill or rejected before unbounded growth.
- Product `cdf run tlc.yellow` over the EC2 full-year local TLC workspace with the opt-in path beats the current appender baseline (`33.955522533s`) and records phase telemetry. Retention target is to materially approach D11's generated handoff median (`9.308415940s`) after unavoidable package/evidence overhead.
- The same product path is measured against the Hugging Face TLC mirror when provider conditions permit; the result is recorded as live provider evidence, not deterministic CI authority.
- Existing append/replace semantics, receipts, checkpoint commits, duplicate re-drive, and cleanup behavior are re-verified under the opt-in path.
- If the opt-in path beats the appender baseline and stays within budget, open a narrow default-promotion ticket; otherwise cancel with evidence and fall back to tuned Parquet handoff or the next G4 candidate.

## References

- `.10x/tickets/done/2026-07-18-p3-d11-duckdb-arrow-ipc-handoff-falsification.md`
- `.10x/tickets/2026-07-11-p3-g4-tlc-remote-io-envelope.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/decisions/destination-runtime-composition-boundary.md`
- `.10x/decisions/compact-lossless-destination-row-provenance.md`

## Assumptions

- Record-backed: D11 proved nanoarrow `read_arrow(...)` works for uncompressed Arrow IPC and fails for LZ4 IPC under the pinned build.
- Record-backed: D11 measured generated full-year TLC-shaped uncompressed IPC handoff at median `9.308415940s`, faster than tuned Parquet handoff median `10.472399505s` and far faster than the persistent appender floor around `32–34s`.
- User-ratified: performance/correctness are the first priority; no potentially regressing default is acceptable without same-host benchmark evidence.
- Record-backed: package identity is not changed in this slice; any identity-bearing bytes still come from the existing package pipeline.

## Journal

- 2026-07-18: Opened from D11's retained conclusion. The first implementation shape is explicit opt-in, DuckDB-owned, and temporary-spill-accounted so it cannot slow the current default while the production path is being proven.
- 2026-07-18: Implemented a DuckDB-destination-owned opt-in prototype locally, behind `CDF_DUCKDB_BULK_PATH=arrow_ipc_read_arrow_handoff`, with uncompressed Arrow IPC handoff files, `_cdf_row_key` materialized before DuckDB, attempt-local staging cleanup, and no generic runtime DuckDB branch. Local focused tests passed, including nanoarrow-available append smoke, receipt verification, duplicate redrive, provenance row-key mapping, and staging cleanup.
- 2026-07-18: EC2 product evidence killed the direction. A first control run accidentally omitted `CDF_BENCH_MEASURE_ENV_JSON`, proving the default appender path was unchanged at `34.014931097s`. Correctly forwarded opt-in env selected the Arrow IPC handoff path: segment staging moved out of the appender floor (`destination_ingress=2.668602s`), but DuckDB final materialization through `INSERT INTO target SELECT ... FROM read_arrow(...)` moved the cost into final binding (`destination_write_receipt=34.808246s`) and worsened total wall to `38.624126722s`.
- 2026-07-18: Tried the only credible product rescue without changing generic orchestration: DuckDB-owned `CREATE TABLE AS SELECT ... FROM read_arrow(...)` for replace/absent append. Full not-null restoration still measured `37.274033174s`; row-key-only not-null restoration still measured `37.204744317s`; forcing `CDF_DUCKDB_THREADS=16` with `CDF_DUCKDB_MEMORY_LIMIT=12GiB` timed out at the `179s` worker guard. The prototype code was fully removed before commit so the repository does not retain a slow legacy path.

## Blockers

Cancelled by the ticket's own retention threshold. The opt-in production path did not beat the current appender baseline and did not approach D11's generated-handoff roofline once integrated into CDF's staged destination lifecycle.

## Evidence

- `.10x/evidence/.storage/2026-07-18-p3-d12-ec2-local-arrow-ipc-handoff-measured.json` — accidental default-control run; appender path unchanged at `34.014931097s`.
- `.10x/evidence/.storage/2026-07-18-p3-d12-ec2-local-arrow-ipc-handoff-optin-measured.json` — opt-in Arrow IPC handoff with `INSERT INTO ... SELECT read_arrow(...)`; `38.624126722s`, slower than appender.
- `.10x/evidence/.storage/2026-07-18-p3-d12-ec2-local-arrow-ipc-handoff-ctas-optin-measured.json` — opt-in CTAS prototype with full not-null restoration; `37.274033174s`, slower than appender.
- `.10x/evidence/.storage/2026-07-18-p3-d12-ec2-local-arrow-ipc-handoff-ctas-rowkey-only-optin-measured.json` — opt-in CTAS prototype with only `_cdf_row_key` not-null restoration; `37.204744317s`, slower than appender.
- `.10x/evidence/.storage/2026-07-18-p3-d12-ec2-local-arrow-ipc-handoff-ctas-rowkey-only-threads16-measured.json` — opt-in CTAS prototype with `CDF_DUCKDB_THREADS=16` and `CDF_DUCKDB_MEMORY_LIMIT=12GiB`; failed at the `179s` worker timeout.
- `.10x/evidence/.storage/2026-07-18-p3-d12-ec2-arrow-ipc-handoff-revision.env`
- `.10x/evidence/.storage/2026-07-18-p3-d12-ec2-arrow-ipc-handoff-build.env`

## Review

Pass for cancellation. The code under test stayed behind the DuckDB destination boundary and did not mutate package identity or common runtime contracts, but its product evidence violated the ticket's explicit retention threshold. No slow opt-in path or disabled branch remains in the source tree. G4/WS-D should fall back to the tuned Parquet handoff candidate or a new higher-leverage materialization strategy.

## Retrospective

D11's generated `CREATE TABLE AS SELECT read_arrow(...)` result was a useful falsification probe but not sufficient product evidence. The integration point matters: CDF's production destination final binding, transaction shape, resource settings, and table/provenance requirements changed the actual DuckDB cost profile. The durable lesson is to require full-CDF EC2 evidence before retaining even an off-by-default destination path. The next G4 branch should favor tuned Parquet handoff because it already has same-host generated evidence around `10.47s`, or it should attack package/destination duplication more directly rather than adding another materialization variant.
