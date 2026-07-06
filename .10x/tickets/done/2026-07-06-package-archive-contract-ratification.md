Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/done/2026-07-05-singer-airbyte-and-package-archive.md
Depends-On: .10x/tickets/done/2026-07-06-package-archive-transcode-primitive.md

# Ratify persisted package archive contract

## Scope

Ratify the persisted `firn package archive` contract before implementing archive file writes, manifest mutation, or CLI behavior. This ticket owns shaping and spec updates only; it does not own source code changes.

## Record-backed context

The book and active package spec require `firn package archive` to transcode `data/` Arrow IPC to Parquet with a fidelity report, keep Arrow IPC canonical, record both forms and hashes in package metadata, and prefer IPC for replay when present.

The completed primitive `.10x/tickets/done/2026-07-06-package-archive-transcode-primitive.md` verifies packages and returns deterministic in-memory Parquet bytes plus per-segment source/archive hashes. It explicitly excludes CLI wiring, archive file placement, manifest archive metadata schema, package lifecycle mutation, and archive file writes.

Current source has no archive metadata in `PackageManifest` or `ManifestIdentity`, no `archive/` directory in the required layout, and replay rejects status `archived`. Implementing persistence now would invent package identity, lifecycle, and CLI semantics.

## Acceptance criteria

- `.10x/specs/package-lifecycle-determinism.md` explicitly defines archive file placement and path naming.
- The manifest/package metadata schema for archived forms, hashes, byte counts, row counts, and fidelity report content is explicit.
- The contract states whether archive metadata participates in `manifest.identity`, `package_hash`, and signature signing input.
- The lifecycle behavior is explicit, including whether `firn package archive` changes package status and how that interacts with replay preferring IPC when present.
- Rerun/idempotency, overwrite/skip behavior, partial-write cleanup, and crash-safety expectations are explicit.
- The CLI contract for arguments, JSON output, human output, and exit behavior is explicit enough to open an executable implementation child.

## Evidence expectations

Record the ratified spec diff and open the bounded implementation ticket that references it. If user ratification supersedes active package decisions, record the superseding decision path.

## Explicit exclusions

No Rust source edits, no package archive file writes, no manifest mutation, no CLI command wiring, and no quality-gate run.

## Progress and notes

- 2026-07-06: Opened after read-only parent/subagent inspection found archive persistence still underspecified. Record-backed behavior covers in-memory transcode and high-level archive intent, but not the persisted metadata and lifecycle contract needed for implementation.
- 2026-07-06: Ratified the persisted archive contract in `.10x/specs/package-lifecycle-determinism.md`: `archive/parquet/data/<segment_id>.parquet`, `archive/parquet/fidelity.json`, top-level non-identity `manifest.archives.parquet` metadata, unchanged `manifest.identity`/`package_hash`/signature signing input, status-preserving archive behavior, replay preference for IPC, manifest-last crash safety, idempotent rerun semantics, `--force` replacement, and `firn package archive <PACKAGE_DIR> [--format parquet] [--force] [--json]` CLI output/exit behavior.
- 2026-07-06: Opened executable implementation child `.10x/tickets/done/2026-07-06-package-archive-persistence-cli.md`.
- 2026-07-06: Evidence recorded in `.10x/evidence/2026-07-06-package-archive-contract-ratification.md`; closure review recorded in `.10x/reviews/2026-07-06-package-archive-contract-ratification-review.md`.

## Blockers

None.
