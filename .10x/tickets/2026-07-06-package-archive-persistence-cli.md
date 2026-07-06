Status: open
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-singer-airbyte-and-package-archive.md
Depends-On: .10x/tickets/done/2026-07-06-package-archive-transcode-primitive.md, .10x/tickets/done/2026-07-06-package-archive-contract-ratification.md

# Implement persisted package archive and CLI

## Scope

Implement the persisted `firn package archive` surface ratified by `.10x/specs/package-lifecycle-determinism.md`. Owns `crates/firn-package/**` archive persistence, archive metadata verification, and `crates/firn-cli/**` command parsing/help/output for `firn package archive <PACKAGE_DIR> [--format parquet] [--force] [--json]`.

## Acceptance criteria

- `firn-package` writes Parquet sidecars to `archive/parquet/data/<segment_id>.parquet` and writes canonical JSON fidelity metadata to `archive/parquet/fidelity.json`.
- `manifest.json` records top-level non-identity `archives.parquet` metadata with source/archive paths, byte counts, SHA-256 hashes, row counts, fidelity report path, and fidelity statement.
- Archive writes do not change `manifest.identity`, `package_hash`, receipt package hashes, signature signing input, lifecycle status, or IPC replay preference.
- `PackageReader::verify` or the package verification path validates present archive metadata and reports tampered, missing, source-mismatched, or orphaned archive sidecars distinctly.
- Archive runs refuse `planned`, `extracting`, `validated`, `loading`, and `archived` packages and allow `packaged`, `loaded`, `committed`, and `checkpointed` packages.
- Archive writes use an operation-scoped temporary path under `archive/.tmp/`, install final files before atomically rewriting `manifest.json`, ignore stale temp paths during replay/verification, and preserve the previous archive metadata if a write fails.
- Clean reruns exit successfully with status `skipped`; mismatched or orphaned existing archives fail by default; `--force` replaces the Parquet archive after canonical package verification succeeds.
- `firn package archive <PACKAGE_DIR> [--format parquet] [--force] [--json]` parses, appears in help, rejects unsupported formats, prints concise human output, and emits JSON with command, package hash, format, status, fidelity report path, fidelity statement, and per-segment archive metadata.
- Existing `firn package verify`, `PackageReader::replay_view`, and `PackageReader::read_segment` continue to prefer and validate canonical IPC package identity.
- The implementation preserves the supply-chain constraint from `.10x/tickets/done/2026-07-06-package-archive-transcode-primitive.md`: do not add the direct arrow-rs `parquet` crate or a dependency path that reintroduces `RUSTSEC-2024-0436`.

## Evidence expectations

Record focused tests for persisted archive layout, manifest archive metadata, package hash/signing input/status non-mutation, archive verification failures, replay after archiving, clean rerun skip, `--force` replacement, unsupported format errors, human output, JSON output, and crash/partial-write cleanup behavior. Run the relevant `QUALITY.md` gates in parallel where safe, including formatting, clippy, targeted package/CLI tests, supply-chain checks, secret scans, and reused CodeQL evidence without recreating the database unnecessarily.

## Explicit exclusions

No Parquet-only replay, archive deletion command, GC policy change, status-to-`archived` behavior change, destination commit change, non-Parquet archive format, package signing implementation, or CI workflow change.

## Progress and notes

- 2026-07-06: Opened after `.10x/tickets/done/2026-07-06-package-archive-contract-ratification.md` made the persisted archive contract executable. A worker should read `.10x/specs/package-lifecycle-determinism.md`, the two dependency tickets, and the current `firn-package`/`firn-cli` code before editing.

## Blockers

None.
