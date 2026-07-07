Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-p0-structural-debt-program.md
Depends-On: .10x/tickets/done/2026-07-07-run-spine-implementation-program.md

# P0 Workstream A: Stream CommitSession segments

## Scope

Ratify and implement the kernel destination session shape required by `VISION.md` 14.1 and `.10x/decisions/p0-structural-debt-stop-line.md`.

Owns:

- `crates/cdf-kernel/src/destination.rs` and focused kernel tests.
- DuckDB, Parquet, and Postgres destination session implementations and tests.
- Destination conformance updates needed to exercise the session contract.
- Focused package/reader helpers only if needed to feed recorded package segments into the session API.
- Decision/spec updates for the exact API shape before implementation.

## Required outcome

- `CommitSession` accepts package segments incrementally, either by a segment stream or by per-segment write calls returning `SegmentAck`.
- Fully materialized replay feeds recorded package segments through the same API that future streaming package-to-destination commit will use.
- `finalize` produces the receipt over everything written.
- `abort` remains the failure path where a destination can abort.
- `DestinationProtocol::begin` is required and no longer has an error-returning default implementation.
- The kernel destination protocol exposes trait-level receipt verification, such as `verify(&self, receipt) -> Result<ReceiptVerification>`, so recovery and replay call verification through the trait.
- DuckDB, Parquet, and Postgres preserve receipt content, idempotency, duplicate handling, package identity, and existing delivery-guarantee semantics.

## Acceptance criteria

- A kernel API decision record cites `VISION.md` 14.1, the P0 stop-line decision, alternatives considered, selected segment-write shape, and compatibility/migration consequences.
- `cdf-kernel` tests prove `begin` is required, segment acks are returned for written segments, and trait-level verification is callable through a `dyn DestinationProtocol`.
- DuckDB, Parquet, and Postgres sessions pass existing focused destination tests and new segment-by-segment session tests.
- A demonstration test drives one package through the session API segment-by-segment and proves byte-identical destination input or equivalent receipt/package identity to the materialized replay path.
- Golden/conformance evidence is updated only when the API change legitimately changes package or receipt artifacts, with the reason recorded.

## Evidence expectations

Record focused kernel/destination tests, conformance output, golden rerun output, jscpd and rust-code-analysis metrics for touched destination/kernel paths, and an adversarial review.

## Explicit exclusions

No new destination type, no new package format, no public performance claim, no distributed scheduler, no streaming supervisor, and no semantic weakening of the commit gate.

## Progress and notes

- 2026-07-07: Opened from P0 stop-line. Current source inspection shows `CommitSession::write(&mut self)` takes no data and `DestinationProtocol::begin` has an error-returning default in `crates/cdf-kernel/src/destination.rs`.
- 2026-07-07: Read-only subagent inventory confirmed eager package materialization paths: `PackageReader::read_all_segments()` in `crates/cdf-package/src/reader.rs`, rows vectors in `crates/cdf-dest-duckdb/src/package.rs` and `crates/cdf-dest-postgres/src/package.rs`, and all-segment Parquet package handling in `crates/cdf-dest-parquet/src/package.rs`.

## Blockers

None. The exact API shape decision is owned by this ticket and MUST be written before implementation edits.
