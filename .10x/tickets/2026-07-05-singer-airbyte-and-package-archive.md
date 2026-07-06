Status: active
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-formats-and-subprocess.md, .10x/tickets/done/2026-07-05-package-builder-reader.md

# Implement Singer/Airbyte adapters and package archive

## Scope

Implement fast-follow Singer and Airbyte source adapters over the subprocess machinery, `ForeignState` handling, protocol parsers, state mapping, and `firn package archive` Parquet transcode with fidelity report. Owns parser additions in `firn-subprocess`/`firn-formats` and archive additions in `firn-package`/CLI.

## Acceptance criteria

- Singer `SCHEMA`, `RECORD`, and `STATE` map to descriptors, batches, and scoped `ForeignState`.
- Airbyte catalog and per-stream state map cleanly into descriptors and positions where possible.
- Foreign state is committed only under the firn-line invariant.
- `package archive` transcodes IPC data to Parquet with fidelity report while preserving canonical IPC identity rules.

## Evidence expectations

Record protocol parser tests, malformed protocol fuzz/property tests, state mapping tests, archive fidelity tests, and CLI archive tests.

## Explicit exclusions

Airbyte destinations remain out of scope.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-06: Split `.10x/tickets/done/2026-07-06-singer-airbyte-protocol-adapters.md` for executable Singer/Airbyte protocol parsing and opaque `ForeignState` mapping. `firn package archive` remains intentionally excluded from that child because Parquet transcode intersects the active supply-chain blocker.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-singer-airbyte-protocol-adapters.md` with focused Singer/Airbyte parser, per-stream batch conversion, canonical opaque `ForeignState` hashing, package write/replay tests, mutation-clean adapter tests, and QUALITY evidence. This parent remains open for `firn package archive`.
- 2026-07-06: Split package archive transcode into executable child `.10x/tickets/done/2026-07-06-package-archive-transcode-primitive.md`. The child owns the supply-chain-clean IPC-to-Parquet primitive and in-memory fidelity report data model. Archive file placement, manifest archive metadata, and CLI wiring remain in this parent after the primitive proof.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-package-archive-transcode-primitive.md` with `firn-package::archive_package_to_parquet`, canonical IPC fidelity report metadata, DuckDB-backed Parquet bytes without the direct arrow-rs `parquet`/`paste` path, destination writer delegation, mutation-clean focused tests, and full QUALITY evidence. This parent remains open for CLI wiring, archive file placement, and manifest archive metadata.
- 2026-07-06: Read-only parent/subagent inspection found persisted archive behavior was not yet executable without inventing semantics. Opened the now-closed shaping ticket `.10x/tickets/done/2026-07-06-package-archive-contract-ratification.md` for archive file placement, manifest metadata, identity/hash participation, lifecycle behavior, rerun/crash policy, and CLI contract ratification.
- 2026-07-06: Closed ratification ticket `.10x/tickets/done/2026-07-06-package-archive-contract-ratification.md` after updating `.10x/specs/package-lifecycle-determinism.md` with the persisted archive contract. Opened executable child `.10x/tickets/2026-07-06-package-archive-persistence-cli.md` for the source implementation.

## Blockers

None. Completion depends on `.10x/tickets/2026-07-06-package-archive-persistence-cli.md`.
