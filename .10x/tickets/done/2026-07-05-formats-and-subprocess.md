Status: done
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/done/2026-07-05-contract-compiler-normalization.md

# Implement formats and subprocess adapters

## Scope

Implement `firn-formats` for Arrow IPC, NDJSON, CSV, and JSON adapters needed by MVP, plus `firn-subprocess` supervision for Arrow IPC and NDJSON stdout adapters with stderr tracing and timeout/exit-code mapping. Owns `crates/firn-formats/**` and `crates/firn-subprocess/**`.

Parquet file-source support was split to `.10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md` after quality scanners showed the direct `parquet` crate would introduce `RUSTSEC-2024-0436` through `paste`.

## Acceptance criteria

- Arrow IPC streams convert into kernel batches without schema loss.
- NDJSON inference feeds the same contract path as row-shaped authoring.
- CSV and JSON file sources produce resource descriptors and batches for MVP file sources.
- Subprocess exit, timeout, and malformed output map into the shared taxonomy.
- Adapter output can be packaged and replayed like native output.

## Evidence expectations

Record parser round-trip tests, malformed input tests, subprocess supervision tests, and package integration tests.

## Explicit exclusions

Singer and Airbyte parsers are fast-follow and owned by `.10x/tickets/2026-07-05-singer-airbyte-and-package-archive.md`.

Parquet file-source support is split out and blocked under `.10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md` until the supply-chain policy is ratified or an alternative reader avoids `RUSTSEC-2024-0436`.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-06: Assigned to formats/subprocess worker after contract compiler closure. Worker owns `crates/firn-formats/**` and `crates/firn-subprocess/**` and may propose minimal shared type additions only when required by active specs; leave unrelated dirty `.gitignore` untouched.
- 2026-07-06: Implemented Arrow IPC stream, NDJSON, CSV, and JSON adapters in `firn-formats`; implemented supervised Arrow IPC/NDJSON stdout subprocess adapters with stderr trace capture and timeout/exit/malformed-output error mapping in `firn-subprocess`. No shared type additions were required.
- 2026-07-06: `parquet = "59.0.0"` initially implemented file-source Parquet reads, but `cargo deny check advisories` and OSV reported `RUSTSEC-2024-0436` via the transitive `paste` dependency. The direct dependency and reader were removed before closure. `FileFormat::Parquet` now reports a contract error naming the supply-chain blocker, and the unresolved MVP Parquet requirement is owned by `.10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md`.
- 2026-07-06: Evidence recorded in `.10x/evidence/2026-07-06-formats-and-subprocess.md`. Targeted format/subprocess tests, package compatibility tests, format command, clippy with warnings denied, and `cargo deny check advisories` passed after the Parquet split. Closure review recorded in `.10x/reviews/2026-07-06-formats-and-subprocess-review.md`.
- 2026-07-06: Workspace quality gates for the engine/declarative/formats batch are recorded in `.10x/evidence/2026-07-06-engine-declarative-formats-quality-gates.md`.

## Blockers

None.
