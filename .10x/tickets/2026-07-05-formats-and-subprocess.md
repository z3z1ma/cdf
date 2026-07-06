Status: open
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/2026-07-05-contract-compiler-normalization.md

# Implement formats and subprocess adapters

## Scope

Implement `firn-formats` for Arrow IPC, NDJSON, CSV, JSON, and Parquet adapters needed by MVP, plus `firn-subprocess` supervision for Arrow IPC and NDJSON stdout adapters with stderr tracing and timeout/exit-code mapping. Owns `crates/firn-formats/**` and `crates/firn-subprocess/**`.

## Acceptance criteria

- Arrow IPC streams convert into kernel batches without schema loss.
- NDJSON inference feeds the same contract path as row-shaped authoring.
- CSV/JSON/Parquet file sources produce resource descriptors and batches for MVP file sources.
- Subprocess exit, timeout, and malformed output map into the shared taxonomy.
- Adapter output can be packaged and replayed like native output.

## Evidence expectations

Record parser round-trip tests, malformed input tests, subprocess supervision tests, and package integration tests.

## Explicit exclusions

Singer and Airbyte parsers are fast-follow and owned by `.10x/tickets/2026-07-05-singer-airbyte-and-package-archive.md`.

## Progress and notes

- 2026-07-05: Opened from book and specs.

## Blockers

None.

