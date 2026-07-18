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
- 2026-07-18: Implemented the first product stream-scan staged-ingress slice inside `cdf-dest-duckdb` only. The crate now owns a raw DuckDB C connection/prepared-statement boundary, a pinned `duckdb_arrow_scan` smoke test, and a manual Arrow C stream adapter over CDF's acknowledgement-bearing durable segment stream. The stream-scan path appends `_cdf_row_key`, validates schema/segment identity/order/row counts during DuckDB pull, acknowledges segments only after the scanner consumes them, and writes target rows plus row-key allocator/mirrors/receipt in one DuckDB transaction. The generic runtime still branches only on the prepared bulk path and has no DuckDB-specific logic.
- 2026-07-18: Kept appender as the default selected path. Stream-scan is exposed as a truthful descriptor and can be selected through the DuckDB-local staged-ingress path preference for tests/benchmarks, but it is not promoted as the default until same-host EC2 TLC evidence beats the current `33.955522533s` baseline. Merge remains on appender during preparation.

## Blockers

EC2 promotion evidence is still required before retaining stream-scan as the default path. Local correctness is green; full TLC local/HF benchmark cells are pending.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-duckdb stream_scan --locked -j 12` — passed. Covers the pinned raw `duckdb_arrow_scan` smoke and forced stream-scan staged-ingress receipt/provenance test.
- `cargo fmt --check && CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-duckdb --locked -j 12` — passed. Covers appender, forced stream-scan append, merge appender compatibility, duplicate replay, replace, rollback-adjacent abort paths, correction readback, destination conformance, and native-resource tests.

## Review

Pending.

## Retrospective

Pending.
