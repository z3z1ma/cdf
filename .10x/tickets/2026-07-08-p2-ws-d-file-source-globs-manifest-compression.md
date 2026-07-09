Status: open
Created: 2026-07-08
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-data-onramp-program.md
Depends-On: .10x/decisions/data-onramp-file-source-transport-manifest.md, .10x/specs/data-onramp-file-sources-transports.md

# P2 WS-D file source globs, manifests, and compression

## Scope

Grow the file source from one local file into partitioned globs with default `FileManifest` incrementality, transparent compression, format auto-detection, and contract-governed per-file schema variance.

Split executable child tickets before code for glob partition planning, manifest state comparison, no-op reruns, gzip/zstd streaming, format detection, preview/run parity, and schema variance.

## Acceptance criteria

- Multi-file globs no longer fail run; they plan deterministic file partitions.
- Preview and run share file resolution and decode/schema/normalization front-end behavior.
- First run records `FileManifest`; subsequent runs plan only new/changed files; unchanged reruns are fast no-ops.
- Gzip and zstd decode transparently in streaming mode.
- Format auto-detection confirms extension with magic bytes and fails clearly on mismatch.
- Mixed file schemas are governed by contract policy rather than unclassified crashes.

## Evidence expectations

Conformance S2/S3/S8 fixtures, manifest state evidence, no-op rerun evidence, compression tests, source-position package evidence, and adversarial review of replay/checkpoint determinism.

## Explicit exclusions

Zip archive member semantics and large-N coalescing thresholds are deferred unless a child ticket ratifies them.

## Progress and notes

- 2026-07-08: Opened as P2 workstream owner from the directive.
- 2026-07-08: Split first executable child `.10x/tickets/2026-07-08-p2-ws-d1-file-glob-partition-planning.md` for modest-N local glob partition planning and preview/run file-resolution parity.
- 2026-07-09: D1 closed as `.10x/tickets/done/2026-07-08-p2-ws-d1-file-glob-partition-planning.md` with evidence in `.10x/evidence/2026-07-09-p2-ws-d1-file-glob-partition-planning.md` and review in `.10x/reviews/2026-07-09-p2-ws-d1-file-glob-partition-planning-review.md`. Local modest-N globs now plan one deterministic root-relative file partition per match and preview/run open the selected partition through the same validation path. Manifest incrementality, no-op reruns, compression, remotes, schema variance, and large-N coalescing remain open.

## Blockers

None for modest-N file partitions. Large-N coalescing needs a child decision before implementation.
