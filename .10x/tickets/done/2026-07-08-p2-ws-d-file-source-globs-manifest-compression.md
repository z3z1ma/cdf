Status: done
Created: 2026-07-08
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-08-p2-data-onramp-program.md
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
- 2026-07-08: Split first executable child `.10x/tickets/done/2026-07-08-p2-ws-d1-file-glob-partition-planning.md` for modest-N local glob partition planning and preview/run file-resolution parity.
- 2026-07-09: D1 closed as `.10x/tickets/done/2026-07-08-p2-ws-d1-file-glob-partition-planning.md` with evidence in `.10x/evidence/2026-07-09-p2-ws-d1-file-glob-partition-planning.md` and review in `.10x/reviews/2026-07-09-p2-ws-d1-file-glob-partition-planning-review.md`. Local modest-N globs now plan one deterministic root-relative file partition per match and preview/run open the selected partition through the same validation path. Manifest incrementality, no-op reruns, compression, remotes, schema variance, and large-N coalescing remain open.
- 2026-07-09: Split D2 child, now closed as `.10x/tickets/done/2026-07-09-p2-ws-d2-file-manifest-run-aggregation.md`, for aggregating per-file segment `FileManifest` positions into one resource-level checkpoint manifest. Manifest filtering and no-op reruns remain later children.
- 2026-07-09: D2 closed as `.10x/tickets/done/2026-07-09-p2-ws-d2-file-manifest-run-aggregation.md` with evidence in `.10x/evidence/2026-07-09-p2-ws-d2-file-manifest-run-aggregation.md` and review in `.10x/reviews/2026-07-09-p2-ws-d2-file-manifest-run-aggregation-review.md`. Local multi-file project runs now commit a resource-level `FileManifest` with stable source-root-relative file paths while retaining per-segment file evidence. Manifest comparison/filtering and no-op reruns remain open.
- 2026-07-09: Split executable child `.10x/tickets/done/2026-07-09-p2-ws-d3-file-manifest-incremental-noop.md` for local append `FileManifest` filtering and unchanged-run no-op behavior.
- 2026-07-09: D3 closed as `.10x/tickets/done/2026-07-09-p2-ws-d3-file-manifest-incremental-noop.md` with evidence in `.10x/evidence/2026-07-09-p2-ws-a7-d3-i2-batch.md` and review in `.10x/reviews/2026-07-09-p2-ws-a7-d3-i2-batch-review.md`. Local append file resources now skip unchanged committed files as an explicit no-op and load only new or changed files, while replace disposition still plans every file. Remote/public file identities, compression, schema variance, large-N coalescing, and S2/S3/S8 final conformance remain open.
- 2026-07-09: Split executable child `.10x/tickets/done/2026-07-09-p2-ws-d4-gzip-zstd-file-decode-foundation.md` for local gzip/zstd NDJSON decode foundation.
- 2026-07-09: D4 closed as `.10x/tickets/done/2026-07-09-p2-ws-d4-gzip-zstd-file-decode-foundation.md` with evidence in `.10x/evidence/2026-07-09-p2-ws-d4-gzip-zstd-file-decode-foundation.md` and review in `.10x/reviews/2026-07-09-p2-ws-d4-gzip-zstd-file-decode-foundation-review.md`. Local gzip/zstd NDJSON resources now decode through the file runtime with compression metadata on relevant file partitions; remote compressed reads, zip, cloud stores, Parquet byte-stream compression, and final S3 conformance remain open under later WS-D/WS-E/WS-I work.
- 2026-07-09: Split executable child, now terminal at `.10x/tickets/done/2026-07-09-p2-ws-d5-binary-format-autodetection.md`, for unambiguous `.parquet`/`.arrow` extension-plus-magic inference. Text formats and additional aliases remain explicit checkpoints rather than inferred conventions.
- 2026-07-09: D5 closed as `.10x/tickets/done/2026-07-09-p2-ws-d5-binary-format-autodetection.md` with evidence in `.10x/evidence/2026-07-09-p2-d5-i5-integration.md` and review in `.10x/reviews/2026-07-09-p2-d5-i5-integration-review.md`. Canonical Parquet and Arrow IPC file formats now infer at the resource/glob plan, every local match is confirmed by magic, HTTPS Parquet confirmation is bounded, and unsupported HTTPS Arrow fails during plan/deep validation. Text/alias inference, remote Arrow, schema variance, large-N policy, and final S2/S3/S8 conformance remain open.
- 2026-07-10: Remote gzip/zstd row formats, typed per-file schema variance, HTTP/cloud manifests, and S2/S3/S8 conformance closed through WS-E/A/I integrations. `.10x/decisions/logical-file-partitions-executor-packing-and-zip-trigger.md` resolves large-N packing without changing logical partitions and records the zip activation boundary. Aggregate evidence: `.10x/evidence/2026-07-10-p2-ws-d-file-source-closure.md`. Review: `.10x/reviews/2026-07-10-p2-ws-d-file-source-closure-review.md`.

## Blockers

None.
