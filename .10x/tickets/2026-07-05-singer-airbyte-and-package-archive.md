Status: open
Created: 2026-07-05
Updated: 2026-07-05
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/2026-07-05-formats-and-subprocess.md, .10x/tickets/done/2026-07-05-package-builder-reader.md

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

## Blockers

None.
