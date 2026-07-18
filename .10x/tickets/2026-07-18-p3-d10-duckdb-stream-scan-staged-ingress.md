Status: active
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-11-p3-d2-duckdb-arrow-bulk.md, .10x/tickets/2026-07-11-p3-g4-tlc-remote-io-envelope.md

# P3 D10: DuckDB stream-scan staged ingress

## Scope

Implement a DuckDB-crate-owned staged-ingress bulk path that materializes eligible append/replace durable segment streams through DuckDB's Arrow stream scanner, preserving the existing `_cdf_row_key` payload column and CDF mirror/receipt semantics. Select the path through destination bulk preparation/capabilities only; generic runtime must not name DuckDB or path ids.

## Non-goals

- No generic runtime destination branch.
- No use of the Rust binding's process-global ArrowVTab retention path.
- No removal or redesign of `RowProvenanceAddress`.
- No merge stream-scan path until separately measured; merge may keep the existing Arrow appender compatibility path.
- No old DuckDB artifact migration or compatibility shim.

## Acceptance Criteria

- DuckDB runtime capabilities expose truthful bulk descriptors for the stream-scan path and appender compatibility path, with append/replace selecting stream-scan and merge selecting appender during preparation.
- Stream-scan staged ingress consumes the existing durable segment stream, validates canonical segment identity/schema/row counts, appends `_cdf_row_key`, acknowledges segments only after transferring them to DuckDB scanner ownership, and commits mirrors/receipt in one transaction.
- `_cdf_row_key`, `_cdf_segments`, duplicate package-token handling, append, replace, rollback, receipt verification, and correction readback remain equivalent to the current contract.
- DuckDB-specific raw C API and unsafe Arrow C stream code live only in `cdf-dest-duckdb`, with safety comments and pinned-version tests.
- Local tests cover the selected path, forced appender compatibility for merge, rollback on stream error, duplicate replay, and extension-boundary invariants.
- EC2 evidence records full CDF local TLC-to-DuckDB and HF TLC-to-DuckDB after the change. Retain the default only if it beats the current host-labeled `33.955522533s` local baseline and does not regress correctness/conformance.

## References

- `.10x/decisions/duckdb-stream-scan-staged-ingress.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/decisions/compact-lossless-destination-row-provenance.md`
- `.10x/decisions/destination-runtime-composition-boundary.md`
- `.10x/tickets/2026-07-11-p3-g4-tlc-remote-io-envelope.md`

## Assumptions

- Record-backed: EC2 lab evidence shows `_cdf_row_key` stream-scan materialization at median `5.111650191s` with `threads=16` and `1GiB` DuckDB memory/temp limits.
- Record-backed: `duckdb_arrow_scan` is deprecated in the pinned C API, so use must be isolated behind the destination crate and pinned tests.
- User-ratified: performance and correctness are joint first priority; no potentially regressing default is retained without same-host evidence.

## Journal

- 2026-07-18: Opened after G4 showed that `_cdf_row_key` stream-scan under bounded parallel DuckDB settings beats the appender floor by roughly `6.64x`, while preserving the current provenance column. The implementation seam is destination bulk preparation and staged ingress; generic runtime remains destination-neutral.

## Blockers

None after the stream-scan decision. If raw C transaction integration proves larger than this ticket, split before retaining partial product behavior.

## Evidence

Pending implementation.

## Review

Pending.

## Retrospective

Pending.
