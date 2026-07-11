Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-performance-investigation-backlog.md

# Triage streaming package-to-destination commit paths

## Scope

Investigate whether destination commits can consume package data in a streaming or segment-by-segment way instead of fully loading package segments into memory before commit, while preserving package verification, receipts, idempotency, and replay semantics.

This ticket is triage only. It does not authorize changing any destination implementation, package reader API, receipt schema, or checkpoint commit path.

## Current hypothesis

Current destination paths commonly open a package, verify it, read all segments into `Vec<RecordBatch>`-like structures, and then plan/write. That is safe and simple, but it may impose avoidable memory and latency overhead for large packages. A streaming commit interface could reduce peak memory and support larger local packages, but it must not weaken receipt verification or idempotent replay.

## Investigation questions

- Which destinations currently read all package data before writing, and which need full-package knowledge for planning?
- Can package identity verification be separated from eager data materialization?
- Can destination sheets express whether commit supports streaming, bulk ingest, transaction boundaries, and segment-level receipts?
- Can receipts remain package-level while writes are segment-streamed?
- How would failure recovery work if some segments have been streamed into a transaction or object-store prefix before finalization?
- Does streaming commit require package reader API changes, destination trait changes, or only internal destination refactors?

## Destination-specific concerns

- DuckDB: native appender or Arrow table registration may accept batches incrementally, but transaction and mirror/receipt behavior must remain exact.
- Postgres: `COPY` or batched inserts could stream, but merge/upsert may still need staging and dedup semantics.
- Parquet/object-store: segment-to-object streaming should be natural, but receipt manifests must capture final object hashes and duplicate replay behavior.
- Lakehouse destinations: future Delta/Iceberg transaction metadata may prefer file-level streaming but commit atomically at snapshot/finalize time.

## Acceptance criteria

- Inventory current package loading behavior for DuckDB, Postgres, Parquet/object-store, and package archive paths.
- Identify peak-memory risk by destination and package size class.
- Determine whether a shared streaming package reader or destination commit API is necessary.
- State the receipt and failure-recovery invariants any streaming implementation must preserve.
- Recommend no action, destination-specific implementation tickets, or a prerequisite specification update.
- If implementation is recommended, split by destination and do not create one broad rewrite ticket.

## Evidence expectations

- Source inspection of `crates/cdf-dest-duckdb/**`, `crates/cdf-dest-postgres/**`, `crates/cdf-dest-parquet/**`, and `crates/cdf-package/src/reader.rs`.
- At least a qualitative memory model for current all-segments load behavior.
- If measured later, record peak RSS and package size for deterministic fixtures.

## Explicit exclusions

No destination API change, no streaming reader implementation, no receipt schema change, no checkpoint behavior change, no lakehouse implementation, no commit protocol relaxation, and no package verification shortcut.

## References

- `.10x/tickets/2026-07-07-performance-investigation-backlog.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `crates/cdf-package/src/reader.rs`
- `crates/cdf-dest-duckdb/**`
- `crates/cdf-dest-postgres/**`
- `crates/cdf-dest-parquet/**`

## Progress and notes

- 2026-07-07: Opened from performance discussion. The expected risk is peak memory and duplicate IPC readback when packages are immediately committed after being written.
- 2026-07-11: P3 source/architecture audit confirmed whole-package destination materialization. A1/A5 establish durable-segment streaming and final binding; D1–D5 implement/verify bulk destination consumers; F2 audits residual collections. This triage owns no implementation and remains open only until those children attach before/after memory/receipt/crash evidence and the P3 closeout moves it terminal.
- 2026-07-11: WS-L separately measured materialized package build, DuckDB commit, and Parquet commit and preserved their tiny-fixture/setup bias in `.10x/evidence/2026-07-11-p3-l5-preoptimization-baseline.md`. A1/A5/D1-D5/F2 own bounded streaming and receipt/crash before/after proof.

## Blockers

None for investigation. Any shared API change is blocked on a spec/API recommendation and separate implementation ticket.
