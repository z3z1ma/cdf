Status: done
Created: 2026-08-08
Updated: 2026-08-08
Depends-On: `.10x/tickets/done/2026-08-06-u6b-default-live-telemetry.md`

# MongoDB incremental live progress observations

## Scope

Publish bounded process-local MongoDB extraction progress as admitted wire/decode batches complete
so the existing normal-mode live telemetry can show rows, bytes, batches, and phase-local rate
during a long read rather than only at final package publication.

## Non-goals

- durable per-batch run-ledger events;
- fabricated totals or row counts before decode;
- changes to package, checkpoint, receipt, or schema authority;
- connector-specific rendering.

## Acceptance Criteria

- Each completed MongoDB decode batch updates the existing drop-capable live progress sink with
  cumulative phase-local rows/bytes and explicit batch count.
- Observations never block extraction, alter backpressure, double-count packaging/destination
  counters, expose source values, or enter durable identity/evidence.
- TTY and headless tests prove counters advance before extraction completion; quiet, JSON, and
  disabled-progress behavior remain unchanged.
- One release Atlas run shows changing counters during extraction while retaining exact final rows,
  package hash, receipt, and checkpoint.

## References

- `.10x/specs/cli-live-progress.md`
- `.10x/tickets/done/2026-08-06-u6b-default-live-telemetry.md`
- `.10x/tickets/done/2026-08-02-mongodb-source-connector.md`

## Assumptions

- Record-backed: the 417,114-row Atlas portable run emitted elapsed-only heartbeats at 30, 60, and
  90 seconds even though the adapter had completed bounded wire/decode batches; final counters
  appeared only at extraction completion.

## Journal

- 2026-08-08: Opened from the final live Atlas portability certificate. The renderer and clock
  behavior are already correct; the missing authority is an adapter-to-process-local-progress
  observation at the completed decode-batch frontier.
- 2026-08-08: Activated. Source inspection found a stronger existing authority boundary than a
  MongoDB-specific callback: the engine validates and admits every source batch before downstream
  normalization. The repair publishes cumulative rows, bytes, and batches there through the
  existing drop-capable progress sink, improving all batch-producing adapters without durable
  events or connector-owned rendering.
- 2026-08-08: Implemented cumulative source-batch observations at the engine's validated batch
  frontier and mapped them into the existing extract phase. TTY redraw remains bounded, headless
  observations coalesce until the 30-second heartbeat, retry notices remain immediate, and the
  project recorder publishes no durable event. Focused tests and strict affected-package Clippy
  pass; the release Atlas visual certificate remains.
- 2026-08-08: The first release Atlas run correctly showed advancing source counters but falsified
  phase isolation at the durable segment frontier: the existing renderer still classified package
  segment increments as extraction, doubling the final Read rows from 417,114 to 834k. Repaired
  package-segment attribution to a concurrently visible Package phase without ending an active
  Read phase. A focused overlap regression now proves source and package totals never combine.
- 2026-08-08: Rebuilt the bundled-DuckDB release binary at `48140256` and reran the 417,114-row
  Atlas resource in a 160-column PTY. Read counters advanced throughout extraction and completed at
  417k rows, 58 MiB, 51 batches, and 21.0k rows/s; Package separately completed at 417k rows,
  13 MiB, one segment, and one batch. The destination loaded all 417,114 rows and published the
  package hash, receipt, and checkpoint. The exact emitted package id then resolved through
  `cdf inspect package <id>` with no path construction.

## Blockers

None.

## Evidence

- The focused engine test observed exact cumulative rows, bytes, and batch count for every source
  batch before package finalization.
- Two CLI-core tests prove cumulative replacement in normal TTY output and no per-batch headless
  emission before the bounded heartbeat, whose output contains the last-known counters.
- The project recorder test proves the observation reaches the process-local sink while the run
  ledger remains empty.
- The combined affected-package strict Clippy, formatter, and diff checks pass.
- Release binary SHA-256:
  `6bb651e8e8ce779156f8fd51ace34893c25412864b80481e2659925cc58aab9b`.
  The PTY transcript at `/tmp/cdf-atlas-progress-48140256.txt` shows advancing phase-local Read
  metrics, the separate Package summary, final loaded rows, receipt, and checkpoint. This is a
  live external Atlas observation and is therefore temporal rather than a deterministic test.
- `target/release/cdf --project /Users/alexanderbut/code_projects/cdf_sandbox --json inspect
  package pkg-atlas-throughput-depreciation-items-portable-62252-1786251432163398000` returned the
  same exact package id and `sha256:ee99b2aa262fbf4974e00b7c0aa40c278967459618a5aafe22e252840e25ca30`
  with empty stderr.

## Review

Pass. The implementation publishes only cumulative process-local source observations at the
engine's admitted-batch frontier, keeps durable evidence unchanged, and maps package segments to a
separate concurrent phase. Focused regressions cover cumulative replacement, TTY rendering,
bounded headless emission, durable-ledger exclusion, and overlapping source/package counters. The
release run exercised the real MongoDB, package, DuckDB destination, receipt, checkpoint, and
identifier-inspection boundaries.

## Retrospective

The generic admitted-source-batch seam was both smaller and more complete than an adapter callback.
The live run caught a presentation ownership bug that unit coverage initially missed: two correct
cumulative counters become false when assigned to one phase. Progress tests should therefore cover
overlapping phases as well as isolated events. Keeping animation process-local preserved ledger
bounds and made the throughput path observable without introducing hot-path persistence.
