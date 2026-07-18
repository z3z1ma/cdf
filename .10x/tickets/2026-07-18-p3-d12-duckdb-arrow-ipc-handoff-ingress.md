Status: active
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

## Blockers

None for an opt-in implementation. Default promotion is blocked until product EC2 evidence proves the path beats the retained appender baseline and the extension/staging risks are closed.

## Evidence

Pending implementation.

## Review

Pending implementation.

## Retrospective

Pending implementation.
