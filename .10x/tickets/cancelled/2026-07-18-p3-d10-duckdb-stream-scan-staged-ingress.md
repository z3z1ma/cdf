Status: cancelled
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/done/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-11-p3-d2-duckdb-arrow-bulk.md, .10x/tickets/done/2026-07-11-p3-g4-tlc-remote-io-envelope.md

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
- `.10x/tickets/done/2026-07-11-p3-g4-tlc-remote-io-envelope.md`

## Assumptions

- Record-backed: EC2 lab evidence shows `_cdf_row_key` stream-scan materialization at median `5.111650191s` with `threads=16` and `1GiB` DuckDB memory/temp limits.
- Record-backed: `duckdb_arrow_scan` is deprecated in the pinned C API, so use must be isolated behind the destination crate and pinned tests.
- User-ratified: performance and correctness are joint first priority; no potentially regressing default is retained without same-host evidence.

## Journal

- 2026-07-18: Opened after G4 showed that `_cdf_row_key` stream-scan under bounded parallel DuckDB settings beats the appender floor by roughly `6.64x`, while preserving the current provenance column. The implementation seam is destination bulk preparation and staged ingress; generic runtime remains destination-neutral.
- 2026-07-18: Implemented the first product stream-scan staged-ingress slice inside `cdf-dest-duckdb` only. The crate now owns a raw DuckDB C connection/prepared-statement boundary, a pinned `duckdb_arrow_scan` smoke test, and a manual Arrow C stream adapter over CDF's acknowledgement-bearing durable segment stream. The stream-scan path appends `_cdf_row_key`, validates schema/segment identity/order/row counts during DuckDB pull, acknowledges segments only after the scanner consumes them, and writes target rows plus row-key allocator/mirrors/receipt in one DuckDB transaction. The generic runtime still branches only on the prepared bulk path and has no DuckDB-specific logic.
- 2026-07-18: Kept appender as the default selected path. Stream-scan is exposed as a truthful descriptor and can be selected through the DuckDB-local staged-ingress path preference for tests/benchmarks, but it is not promoted as the default until same-host EC2 TLC evidence beats the current `33.955522533s` baseline. Merge remains on appender during preparation.
- 2026-07-18: First EC2 candidate at commit `fbe0b29362aaba5841c3f906e898d763793d6a96` timed out at the worker's `119000ms` child guard. The likely miss was materialization shape: the synthetic 5s reference used DuckDB `CREATE TABLE AS SELECT * FROM arrow_stream`, while the first product path pre-created the table and then executed `INSERT INTO target SELECT * FROM arrow_stream`. Adjusted fresh append/replace stream-scan to use CTAS and leave existing-table append on `INSERT INTO` until separately measured.
- 2026-07-18: CTAS did not fix the product path. The full-year local TLC candidate at `6cae03cdd50770b1e7bde358203d183ad145ea4a` timed out at the worker's `119000ms` child guard, and the one-partition smoke also timed out at `59000ms`. Disabled stream-scan exposure/selection again: runtime capabilities and preparation advertise/select only the measured appender path. D10 was cancelled and must not promote stream-scan without a redesigned staged-stream adapter and passing EC2 evidence.
- 2026-07-18: Post-cancellation no-legacy cleanup removed the disabled product stream-scan branch, the `StagedArrowStream` adapter, the deprecated `duckdb_arrow_scan` registration wrapper, and raw mirror helpers from `cdf-dest-duckdb`. DuckDB staged ingress now has one product writer shape matching its advertised appender-only capability. Lab-only stream-scan references remain in `cdf-benchmarks` as historical/research workloads, not product code.

## Blockers

None. Cancelled by EC2 promotion evidence: the product stream-scan path timed out at full-year and one-partition scale, and the runtime now advertises only the measured appender path.

## Evidence

- Earlier implementation slice: `CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-duckdb stream_scan --locked -j 12` passed with the pinned raw `duckdb_arrow_scan` smoke and a forced stream-scan staged-ingress receipt/provenance test. The forced product test was removed after EC2 rejected the path at scale.
- Earlier implementation slice: `cargo fmt --check && CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-duckdb --locked -j 12` passed with appender, forced stream-scan append, merge appender compatibility, duplicate replay, replace, rollback-adjacent abort paths, correction readback, destination conformance, and native-resource tests.
- `.10x/evidence/.storage/2026-07-18-p3-d10-ec2-stream-scan-local.json` — failed candidate measurement at `fbe0b29362aaba5841c3f906e898d763793d6a96`; status records `CDF command exceeded worker timeout of 119000ms`. This rejects the first pre-created-table stream-scan shape and is not promotion evidence.
- `.10x/evidence/.storage/2026-07-18-p3-d10-ec2-stream-scan-ctas-local.json` — failed CTAS candidate measurement at `6cae03cdd50770b1e7bde358203d183ad145ea4a`; status records `CDF command exceeded worker timeout of 119000ms`.
- `.10x/evidence/.storage/2026-07-18-p3-d10-ec2-stream-scan-onepart.json` — failed one-partition stream-scan smoke at `6cae03cdd50770b1e7bde358203d183ad145ea4a`; status records `CDF command exceeded worker timeout of 59000ms`.
- After disabling stream-scan exposure again: `cargo fmt --check && CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-duckdb --locked -j 12` — passed. Covered the measured appender default and the then-present pinned raw ABI smoke; no runtime descriptor advertised the failed stream-scan path.
- After post-cancellation no-legacy cleanup: `cargo fmt --all && cargo fmt --check && CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-duckdb --locked -j 12` — passed. Covers appender staged ingress, append/replace/merge, duplicate replay, rollback-adjacent abort paths, receipt verification, correction readback, destination conformance, and native-resource tests after deleting the failed stream-scan product remnants.
- `.10x/tickets/2026-07-18-p3-l7-ec2-benchmark-tranche-lifecycle.md` records the clean host refresh at `8d9695a9cd5eefd49a86be0e1448ba4c84ea43ae` after the stream-scan disable/rejection patch and a subsequent cached marker refresh. Strict preflight passed on the dedicated EC2 host; the cancellation decision is based on host-labeled failed candidate measurements plus the retained appender-only default.

## Review

Pass for cancellation. The ticket's own acceptance criterion required retaining the path only if EC2 evidence beat the current host-labeled local baseline without correctness regression. The attempted product path instead timed out at the worker guard for full-year local TLC and for a one-partition smoke. The implementation was made fail-closed by removing descriptor exposure and path selection, then simplified further by deleting the disabled product branch and raw FFI shim from `cdf-dest-duckdb`; generic runtime remains destination-neutral and the measured appender path remains the only advertised DuckDB staged-ingress path. No critical follow-up is hidden inside this terminal ticket: G4 remains the active owner for the remaining DuckDB/package materialization envelope gap.

## Retrospective

The synthetic DuckDB Arrow stream-scan reference was a useful falsification seed, not sufficient product proof. The production adapter added acknowledgement, schema/order validation, row-key construction, transaction/mirror writes, and durable segment-stream lifetimes; that whole shape did not inherit the synthetic reference's 5-second behavior. The durable rule is now stricter: destination bulk alternatives must be measured through the actual CDF staged-ingress contract before being exposed as runtime capabilities, and failed descriptors must be removed rather than left as knobs that look supported.

The raw `duckdb_arrow_scan` ABI smoke did not justify keeping a deprecated, failed product-path shim in `cdf-dest-duckdb` once D10 was cancelled. Stream-scan exploration may remain in the benchmark lab as research code, but any future production revival needs a fresh bounded ticket, a new destination-owned design, and the EC2 host gate up front; do not reopen this failed attempt.
