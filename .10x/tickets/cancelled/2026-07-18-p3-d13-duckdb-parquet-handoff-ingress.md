Status: cancelled
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-18-p3-d11-duckdb-arrow-ipc-handoff-falsification.md, .10x/tickets/cancelled/2026-07-18-p3-d12-duckdb-arrow-ipc-handoff-ingress.md, .10x/tickets/done/2026-07-11-p3-g4-tlc-remote-io-envelope.md, .10x/tickets/2026-07-15-p3-d9-content-reachability-authority.md

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
- `.10x/tickets/done/2026-07-11-p3-g4-tlc-remote-io-envelope.md`
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
- 2026-07-18: Activated implementation. Initial shape: add an explicit DuckDB-owned `CDF_DUCKDB_BULK_PATH=parquet_read_parquet_handoff` selection, write temporary Parquet handoff bytes inside `cdf-dest-duckdb`, use DuckDB native `read_parquet(...)` for final materialization, and leave the default appender path unchanged until full-CDF EC2 evidence proves promotion.
- 2026-07-18: Implemented the opt-in destination-owned path locally behind DuckDB runtime capabilities. Local focused and full `cdf-dest-duckdb` tests passed, including opt-in selection, receipt/idempotency/provenance, duplicate redrive, row-key addressability, and staging cleanup. EC2 product measurement then failed the ticket's retention threshold: the CTAS-plus-row-key-nullability variant measured `37.546207167s`, slower than the `33.955522533s` appender floor. The phase profile isolated the regression to final binding, not source/package work (`destination_write_receipt=32.407414891s`).
- 2026-07-18: Removed the CTAS branch and simplified the prototype to one materialization model: apply the planned DuckDB table DDL first, then `INSERT INTO target SELECT ... FROM read_parquet(...)`, so constraints are enforced during vectorized ingest rather than by post-load `ALTER`. Local full `cdf-dest-duckdb` tests passed again. EC2 product measurement still failed worse at `38.828391414s`, with `destination_write_receipt=33.200610882s`.
- 2026-07-18: Cancelled D13. The product code was removed before commit; only the retention evidence remains. The generated-lab Parquet result from D11 does not survive full CDF destination final-binding semantics, so G4 must move to a different materialization strategy rather than retaining a slower opt-in path.

## Blockers

Cancelled by product evidence. Default promotion is rejected because both measured product variants are slower than the retained appender baseline.

## Evidence

- `.10x/evidence/.storage/2026-07-18-p3-d13-ec2-local-parquet-handoff-optin-measured.json`: EC2 full-year local TLC product run with the opt-in Parquet handoff CTAS variant. One uncontrolled sample on `host-class-649c6f28be3544c8`, 41,169,720 rows, `37.546207167s` wall, `2.948952064 GiB` peak RSS, cgroup peak `11.328344064 GiB`, `destination_ingress=4.770972202s`, `destination_write_receipt=32.407414891s`.
- `.10x/evidence/.storage/2026-07-18-p3-d13-ec2-local-parquet-handoff-insert-optin-measured.json`: EC2 full-year local TLC product run with the simplified planned-table + `INSERT ... read_parquet(...)` variant. One uncontrolled sample on `host-class-649c6f28be3544c8`, 41,169,720 rows, `38.828391414s` wall, `2.824097792 GiB` peak RSS, `destination_ingress=5.226351699s`, `destination_write_receipt=33.200610882s`.
- `CARGO_BUILD_JOBS=10 cargo test -p cdf-dest-duckdb -- --nocapture`: passed locally before source removal for both variants; proves the rejected code met local semantics but does not justify retention after EC2 failure.

## Review

Pass for cancellation. The prototype stayed behind the DuckDB destination boundary and did not change package identity or generic runtime orchestration, but it violated the explicit performance retention threshold. Cancelling is the correct outcome. No product code, compatibility shim, disabled branch, dependency, or runtime capability remains from this failed path.

## Retrospective

D11's generated Parquet handoff benchmark was too optimistic because it did not include the full CDF destination final-binding contract. In product form, both plausible Parquet materializations spend roughly appender-floor time inside DuckDB final binding after CDF has already paid package encode/persist costs. The next G4 move should stop adding destination materialization variants that duplicate package bytes and instead attack the architecture that creates two persistence formats before commit, or measure a more direct DuckDB ingest primitive with full product semantics before implementation.
