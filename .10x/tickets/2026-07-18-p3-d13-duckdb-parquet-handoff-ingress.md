Status: open
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-18-p3-d11-duckdb-arrow-ipc-handoff-falsification.md, .10x/tickets/cancelled/2026-07-18-p3-d12-duckdb-arrow-ipc-handoff-ingress.md, .10x/tickets/2026-07-11-p3-g4-tlc-remote-io-envelope.md, .10x/tickets/2026-07-15-p3-d9-content-reachability-authority.md

# P3 D13: DuckDB Parquet handoff ingress

## Scope

Implement an opt-in DuckDB destination-owned bulk ingress path that writes validated/normalized batches, including `_cdf_row_key`, to bounded temporary Parquet handoff files and materializes the DuckDB target through native `read_parquet(list_of_files)`.

The path must stay behind DuckDB's destination ingress implementation and the existing generic staged-ingress lifecycle. The common runtime may select a declared bulk path; it must not branch on DuckDB, Parquet filenames, handoff internals, or destination identity.

## Non-goals

- No package artifact format change; existing CDF package segments stay the identity-bearing package artifacts.
- No generic runtime branch naming DuckDB, Parquet, handoff files, or SQL shapes.
- No reintroduction of D10 Arrow stream callbacks or D12 nanoarrow/Arrow IPC handoff code.
- No default promotion without same-host full-CDF EC2 evidence.
- No hard-coded performance cap without a knob.
- No unbounded temporary disk growth; handoff bytes must be reserved against the destination/shared spill authority or rejected cleanly.
- No legacy finalized-package fallback branch if the staged handoff path becomes the selected production path.

## Acceptance Criteria

- DuckDB runtime capabilities expose the Parquet handoff path as a destination-owned bulk ingress strategy without weakening the destination-neutral staged-ingress contract.
- Handoff files are deterministic, bounded, temporary Parquet files with `_cdf_row_key` already materialized and row-key ranges recoverable from CDF segment identities.
- Row-group/file sizing is controlled by explicit DuckDB-owned knobs and defaults to the D11-measured good region, not the rejected 64 MiB/oversized-memory point.
- Handoff disk use is reserved/accounted as explicit temporary spill or the run fails cleanly before unbounded growth.
- Product `cdf run tlc.yellow` over the EC2 full-year local TLC workspace with the opt-in path beats the current appender baseline (`33.955522533s`) and materially approaches the tuned generated Parquet handoff median (`10.472399505s`), with phase telemetry recorded.
- The same product path is measured against the Hugging Face TLC mirror when provider conditions permit; the result is recorded as live provider evidence, not deterministic CI authority.
- Append/replace semantics, receipts, checkpoint commits, duplicate redrive, rollback cleanup, and provenance row-key mapping are re-verified under the opt-in path.
- If the opt-in path beats the appender baseline and stays within budget, open a narrow default-promotion ticket; otherwise cancel with evidence and move to the next G4 materialization strategy.

## References

- `.10x/tickets/done/2026-07-18-p3-d11-duckdb-arrow-ipc-handoff-falsification.md`
- `.10x/tickets/cancelled/2026-07-18-p3-d12-duckdb-arrow-ipc-handoff-ingress.md`
- `.10x/tickets/2026-07-11-p3-g4-tlc-remote-io-envelope.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/decisions/destination-runtime-composition-boundary.md`
- `.10x/decisions/compact-lossless-destination-row-provenance.md`

## Assumptions

- Record-backed: D11 measured tuned generated Parquet handoff at median `10.472399505s`, with `128–256 MiB` row-group policy and approximately `2.75 GiB` peak RSS.
- Record-backed: D12 killed DuckDB Arrow IPC/nanoarrow product handoff because full-CDF opt-in runs measured `37–38s` or timed out, despite D11's generated `read_arrow` win.
- User-ratified: performance/correctness are the first priority; no potentially regressing default is acceptable without same-host benchmark evidence.
- Record-backed: package identity is not changed in this slice; any identity-bearing bytes still come from the existing package pipeline.

## Journal

- 2026-07-18: Opened as D12's fallback path. The first implementation should reuse destination-owned staging/bulk capability seams and avoid broad runtime changes.

## Blockers

None for an opt-in implementation. Default promotion is blocked until EC2 product evidence beats the retained appender baseline and cleanup/staging semantics are proven.

## Evidence

Pending implementation.

## Review

Pending implementation.

## Retrospective

Pending implementation.
