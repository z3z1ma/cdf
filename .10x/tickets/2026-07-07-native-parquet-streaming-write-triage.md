Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-performance-investigation-backlog.md

# Triage native Parquet streaming writes

## Scope

Investigate whether CDF's Parquet destination and package archive paths should write Parquet in a streaming, Arrow-native way from package segments rather than buffering whole outputs or taking unnecessary transcode/readback steps.

This is triage only. It does not authorize changing canonical package data, archive semantics, destination receipt fields, dependency policy, object-store behavior, or Parquet writer implementation.

## Current hypothesis

CDF has already ratified native Arrow/DataFusion Parquet policy, and Parquet is a key archive, interchange, file-source, and destination format. Native Parquet code is architecturally correct, but performance depends on how batches are written: whole-package buffering and repeated readback can lose the advantage of Arrow-native data.

## Investigation questions

- Which current Parquet paths are native Arrow/Parquet and which still carry legacy DuckDB-backed assumptions or buffering behavior?
- Does `cdf-dest-parquet` write each package segment independently, and does it require all segments resident in memory?
- Does `cdf package archive` transcode IPC segments one at a time or materialize more than necessary?
- Can object-store uploads stream while still computing SHA-256, byte count, etag, and receipt data?
- What row group sizing, compression, dictionary encoding, and writer properties should be configurable or fixed for deterministic performance?
- Does any desired writer option threaten byte-for-byte determinism or cross-version package/archive stability?

## Acceptance criteria

- Inventory Parquet write paths for destination and archive surfaces and classify them as streaming, segment-buffered, or package-buffered.
- Identify whether current behavior is already acceptable for MVP or whether it creates a large-package ceiling.
- Identify writer-property choices that affect performance and determinism.
- Decide whether a follow-up should target package archive, Parquet destination, object-store upload behavior, or all separately.
- If implementation is recommended, open separate tickets for archive and destination paths with explicit compatibility and verification criteria.

## Evidence expectations

- Source inspection of `crates/cdf-dest-parquet/**`, `crates/cdf-package/src/parquet.rs`, `crates/cdf-formats/**`, and archive command paths.
- Reference `.10x/decisions/native-arrow-datafusion-parquet-policy.md` for dependency/advisory constraints.
- Optional measurement of IPC-to-Parquet throughput and memory usage for deterministic primitive schemas.

## Explicit exclusions

No canonical package format change, no Parquet-only replay, no archive lifecycle semantic change, no dependency exception expansion, no object-store consistency model change, no row group configuration surface, and no implementation before triage closes.

## References

- `.10x/tickets/2026-07-07-performance-investigation-backlog.md`
- `.10x/decisions/native-arrow-datafusion-parquet-policy.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/tickets/done/2026-07-06-native-parquet-writer-archive.md`
- `crates/cdf-dest-parquet/**`
- `crates/cdf-package/src/parquet.rs`

## Progress and notes

- 2026-07-07: Opened from performance discussion. The target architecture is native Arrow/DataFusion Parquet; this ticket asks whether the current native paths are streaming enough to preserve that advantage.
- 2026-07-11: P3 audit confirmed full-batch/package buffering. D1 defines the bounded destination path, D4 owns streaming row-group/multipart Parquet output, and D5/F4 own semantic/throughput/1 TB closeout. This triage owns no implementation and remains open until D4/D5 attach memory and ≥60%-roofline evidence.
- 2026-07-11: WS-L measured the prepared tiny-package Parquet destination at 0.210 MiB/s median with setup bias, recorded in `.10x/evidence/2026-07-11-p3-l5-preoptimization-baseline.md`. D4/D5/F4 own the large-file streaming, roofline, and constant-memory proof.

## Blockers

None for investigation. Implementation is blocked on path inventory and compatibility constraints.
