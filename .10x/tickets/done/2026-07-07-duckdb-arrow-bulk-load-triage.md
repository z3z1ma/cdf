Status: done
Created: 2026-07-07
Updated: 2026-07-13
Parent: .10x/tickets/2026-07-07-performance-investigation-backlog.md

# Triage DuckDB Arrow bulk-load path

## Scope

Investigate whether `cdf-dest-duckdb` should use a more native DuckDB bulk-load path, such as Arrow table registration, appender APIs, prepared vectorized inserts, or another supported mechanism, instead of row-materializing package batches before commit.

This ticket is triage only. It does not authorize changing DuckDB destination behavior, adding dependencies, changing type mappings, or introducing unsafe FFI.

## Current hypothesis

DuckDB should be one of CDF's fastest local-loop destinations, but current package-loading code appears to convert Arrow batches into row-shaped values before commit. That likely sacrifices much of DuckDB's native ingest performance and makes CDF look worse than it should against direct DuckDB workflows.

## Investigation questions

- What exact row materialization happens today in `cdf-dest-duckdb`, and for which dispositions?
- Does the Rust `duckdb` crate expose stable APIs for Arrow import, appender writes, prepared array binding, or `INSERT INTO ... SELECT * FROM arrow_table` style paths?
- Can a faster path preserve current type fidelity decisions, ICU/timezone restrictions, identifier validation, transaction behavior, mirrors, receipts, duplicate replay, and rollback tests?
- Is the bottleneck row conversion, SQL statement construction, transaction boundaries, mirror writes, or package readback?
- Should DuckDB destination keep a fallback row path for unsupported types or old library behavior?
- Are there supply-chain, unsafe, or bundled-DuckDB build-time implications of the preferred bulk path?

## Candidate validation work

- Inspect `cdf-dest-duckdb` row conversion and commit code.
- Compare current commit timing with a prototype or external scratch experiment using DuckDB's fastest available Rust API, if a later active investigation permits temporary scratch code.
- Verify whether Arrow timezone/timestamp constraints remain unchanged under any bulk path.
- Test append, replace, merge, duplicate package replay, and mirror receipt behavior conceptually before recommending implementation.

## Acceptance criteria

- Identify the fastest safe DuckDB ingest API available through current dependencies.
- Quantify or estimate the expected improvement and state what workload it affects.
- Identify any behavior that would become harder: merge deduplication, schema migration, receipt verification, rollback, duplicate no-op, `_cdf_loads`, `_cdf_state`.
- Recommend no action, scratch research, or a bounded implementation ticket for one disposition first.
- If implementation is recommended, specify whether append should be optimized before replace/merge and what conformance tests must protect.

## Evidence expectations

- Source inspection of `crates/cdf-dest-duckdb/src/package.rs`, `rows.rs`, `commit.rs`, `table.rs`, and tests.
- Dependency/API inspection for the current `duckdb` crate version.
- Optional external documentation or local scratch experiment, recorded as research if non-trivial.

## Explicit exclusions

No DuckDB destination rewrite, no unsafe FFI, no type mapping change, no ICU policy change, no transaction/receipt weakening, no merge semantics change, no dependency upgrade, and no implementation before the triage recommendation is recorded.

## References

- `.10x/tickets/2026-07-07-performance-investigation-backlog.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/tickets/done/2026-07-05-duckdb-destination.md`
- `crates/cdf-dest-duckdb/**`

## Progress and notes

- 2026-07-07: Opened from performance discussion. DuckDB comparison is likely unfair to CDF until the destination uses a native-ish bulk path or the row-materialization cost is proven negligible.
- 2026-07-11: P3 audit confirmed scalar appender ingestion and shaped schema-planned driver-owned bulk paths. D1 owns the neutral contract, D2 owns measured Arrow/vtab selection and fallback, and D5 owns guarantee/conformance/throughput closeout. This triage owns no implementation and remains open until D2/D5 attach the measured API choice and ≥1M rows/s/≥5x evidence.
- 2026-07-11: WS-L measured the prepared tiny-package compatibility path at 0.170 MiB/s median with setup bias, recorded in `.10x/evidence/2026-07-11-p3-l5-preoptimization-baseline.md`. D2/D5 own the large-fixture Arrow-native before/after proof.
- 2026-07-13: Closed the triage after its D2/D5 absorbers completed. The pinned DuckDB binding's vtab helper was rejected because it retains batches; the winning implementation is the isolated Arrow C Stream/data-chunk appender behind the neutral staged durable-segment ingress. Scalar ingestion was deleted under the current-format-only policy. Final named-host evidence is 9,274,639 rows/s, 23.77x the removed scalar control and above the ≥1M/≥5x targets, with type/disposition/correction/receipt/crash laws passing through the generic matrix.

## Blockers

None. The investigation recommendation and bounded implementation were completed by D2 and closed through D5.

## Evidence

- `.10x/tickets/done/2026-07-11-p3-d2-duckdb-arrow-bulk.md` records API selection, bounded staging, type support, before/after controls, and the complete DuckDB suite.
- `.10x/tickets/done/2026-07-11-p3-d5-bulk-path-matrix.md` and `.10x/evidence/2026-07-12-p3-d5-destination-matrix.md` record the typed registry matrix, current named-host rate, schema-ineligible cell, duplicate/recovery law, and fourth-destination extension proof.

## Review

Fresh closure review found every triage question resolved by terminal implementation evidence: the scalar materialization bottleneck was replaced, the unsafe retaining vtab alternative was rejected, the native appender preserves the staged single-writer/transaction/receipt boundary, unsupported schemas fail in preflight, and no production scalar fallback remains. Verdict: **pass**.

## Retrospective

The decisive comparison was not “Arrow versus rows” in the abstract; it was whether the binding API retained batches and whether provenance could remain vectorized. Inspecting ownership and measuring the exact removed scalar shape prevented selecting a superficially native vtab path that violated bounded memory.
