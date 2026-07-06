Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-package-archive-contract-ratification.md, .10x/specs/package-lifecycle-determinism.md, .10x/tickets/done/2026-07-06-package-archive-persistence-cli.md

# Package archive contract ratification evidence

## What was observed

The book and active records require `firn package archive` to transcode canonical `data/` Arrow IPC to Parquet with a fidelity report, record both forms and hashes in package metadata, preserve IPC as canonical package data, and prefer IPC for replay when present.

Current source has an in-memory `archive_package_to_parquet` primitive that verifies a package first and returns deterministic per-segment Parquet bytes and source/archive hashes without writing files, mutating `manifest.json`, changing lifecycle status, or changing replay behavior.

Current `PackageManifest` computes `package_hash` from `ManifestIdentity`; lifecycle status is outside identity; `PackageStatus::Archived` is not replayable; existing package layout has no `archive/` directory or archive metadata. These facts make status-preserving, non-identity archive metadata the only contract consistent with D-4/D-10 and current replay semantics.

## Procedure

- Inspected `.10x/specs/package-lifecycle-determinism.md`.
- Inspected `.10x/tickets/done/2026-07-05-singer-airbyte-and-package-archive.md`.
- Inspected `.10x/tickets/done/2026-07-06-package-archive-transcode-primitive.md`.
- Inspected `firn-the-book-of-the-system.md` sections and decisions covering D-4, D-10, package layout, package lifecycle, replay, and `firn package archive`.
- Inspected current `firn-package` model/reader/archive source through parent and subagent reads.
- Updated `.10x/specs/package-lifecycle-determinism.md` to define archive sidecar placement, `manifest.archives.parquet` metadata, identity/hash/signing participation, status behavior, replay preference, rerun/crash behavior, and CLI contract.
- Opened `.10x/tickets/done/2026-07-06-package-archive-persistence-cli.md` as the executable implementation child.
- Ran `rg -n "\\.10x/tickets/2026-07-06-package-archive-contract-ratification\\.md" .10x || true`; it produced no stale references to the moved top-level ticket path.
- Ran `git diff --check -- .10x` and `git diff --cached --check`; both completed without whitespace errors.

## What this supports or challenges

This supports closing the shaping blocker for persisted package archive behavior. The implementation can now proceed without inventing archive path naming, manifest metadata shape, lifecycle effects, replay preference, or CLI output semantics.

## Limits

No Rust source was edited and no archive persistence behavior was executed in this slice. The evidence supports the record/spec state, not implementation correctness.
