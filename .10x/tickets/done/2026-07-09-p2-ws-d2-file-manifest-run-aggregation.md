Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-d-file-source-globs-manifest-compression.md
Depends-On: .10x/tickets/done/2026-07-08-p2-ws-d1-file-glob-partition-planning.md, .10x/decisions/data-onramp-file-source-transport-manifest.md, .10x/specs/data-onramp-file-sources-transports.md

# P2 WS-D2 file manifest run aggregation

## Scope

Make package-producing local multi-file file-resource runs aggregate per-file `SourcePosition::FileManifest` evidence into one deterministic resource-level `FileManifest` checkpoint position.

D1 made local globs plan deterministic per-file partitions and made preview/run use the same selected-file open path. The remaining runtime gap is in `crates/cdf-project/src/runtime/artifacts.rs`: non-cursor state aggregation still expects all segment source positions to be identical, so multiple valid file partitions can fail with "single resource run produced divergent segment source positions" instead of recording the resource manifest required by P2.

This ticket covers the first-run manifest foundation only. It does not implement manifest-state filtering, unchanged no-op reruns, remote manifests, compression, or schema variance.

## Acceptance criteria

- `cdf-project` aggregates non-cursor segment positions whose variants are all `SourcePosition::FileManifest` into one `SourcePosition::FileManifest` output position.
- Aggregated manifest entries are deterministic and root-relative after the existing file-scope normalization path; sort by path and fail closed on duplicate paths with conflicting identity evidence.
- The aggregation preserves file identity evidence already produced by readers, including `size_bytes`, `sha256`, and `etag`.
- Cursor resources continue to use cursor aggregation unchanged; non-file non-cursor resources continue to require identical positions unless a future decision ratifies another aggregation.
- A local multi-file file-resource run through the project runtime commits a checkpoint whose `output_position` manifest lists every loaded file.
- State segments retain per-segment file evidence so replay/recovery can still trace each segment back to its file partition.

## Evidence expectations

- Focused unit coverage for deterministic `FileManifest` aggregation, duplicate conflicting paths, and mixed-variant failure.
- Runtime coverage exercising a local multi-file resource through package/checkpoint creation and asserting the committed `FileManifest`.
- Focused regression coverage proving the old divergent-position error no longer occurs for valid multi-file file manifests.
- Quality gates per `QUALITY.md`, including fmt, clippy, relevant cargo tests, jscpd, complexity, Semgrep, Gitleaks, cargo deny/audit/vet/machete, OSV, and reusable CodeQL when source changes.
- Evidence record and adversarial review before closure.

## Explicit exclusions

- Manifest comparison against previous checkpoint state.
- Fast no-op reruns when no files changed.
- Planning only new/changed files.
- HTTP/S3/GCS/Azure manifest identity.
- gzip/zstd streaming and format auto-detection.
- Per-file schema variance policy.

## Progress and notes

- 2026-07-09: Opened as the next WS-D child after D1. This intentionally precedes manifest filtering/no-op work because checkpoint state must first contain a trustworthy resource-level manifest.
- 2026-07-09: Worker implemented non-cursor all-`FileManifest` aggregation in `cdf-project` runtime artifacts. Aggregation deduplicates identical entries, sorts deterministically by path through `BTreeMap`, preserves `size_bytes`/`etag`/`sha256`, rejects conflicting duplicate path evidence, and leaves cursor/non-file aggregation behavior unchanged.
- 2026-07-09: Added focused runtime tests for deterministic manifest aggregation, conflicting duplicate paths, mixed file/non-file rejection, file-scope normalization, and a live local two-file resource run committing a checkpoint manifest that lists both files while retaining one-file evidence on each state segment.
- 2026-07-09: Worker verification passed: `cargo test -p cdf-project --locked file_manifest`; `cargo test -p cdf-project --locked general_project_run_commits_multi_file_resource_manifest_checkpoint`; `cargo test -p cdf-project --locked state_delta_rejects_mixed_file_and_non_file_source_positions`; `cargo fmt --all -- --check`; `cargo test -p cdf-project --locked`; `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`.
- 2026-07-09: Parent review found the live multi-file test only asserted basenames and would have allowed temp-dir-dependent absolute manifest paths. Patched `cdf-engine` to normalize `FileManifest` source positions at the partition boundary when the partition scope is `ScopeKey::File`, then tightened the live project-runtime assertion to exact source-root-relative paths.
- 2026-07-09: Parent focused verification after the stable-path patch passed: `cargo test -p cdf-engine --locked execution_returns_segment_source_position_evidence`; `cargo test -p cdf-project --locked general_project_run_commits_multi_file_resource_manifest_checkpoint`; `cargo test -p cdf-project --locked file_manifest`; `cargo test -p cdf-project --locked state_delta_rejects_mixed_file_and_non_file_source_positions`.
- 2026-07-09: Closure evidence recorded in `.10x/evidence/2026-07-09-p2-ws-d2-file-manifest-run-aggregation.md`; adversarial review recorded in `.10x/reviews/2026-07-09-p2-ws-d2-file-manifest-run-aggregation-review.md`. Full workspace tests, workspace clippy, fmt, Semgrep, Gitleaks, cargo deny/audit/vet/machete, OSV classification, rust-code-analysis, jscpd, direct unsafe scan, and reusable CodeQL were run. D2 is closed; manifest filtering and no-op reruns remain later WS-D children.

## Blockers

None.
