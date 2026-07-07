Status: active
Created: 2026-07-05
Updated: 2026-07-07

# Packages, lifecycle, and determinism

## Purpose and scope

This specification governs load package layout, lifecycle, crash recovery semantics at package boundaries, retention, hashing, signature readiness, archive behavior, and golden-package determinism. It derives from book Chapters 11 and 19 and decisions D-4, D-10, D-13, D-21, and D-28.

## Package meaning

A load package is the durable, hash-addressed evidence of one attempted state transition for one resource-partition set. Staging is a side effect; evidence is the identity.

Packages MUST contain or reference the plan, observed/output schemas, contract decisions, normalized data, quarantine, stats, lineage, input checkpoint, proposed state delta, destination commit plan, receipts, and trace.

State and destination commit evidence MUST follow `.10x/decisions/package-state-commit-preimage-artifacts.md`: `state/input_checkpoint.json`, `state/proposed_delta.json`, and `destination/commit_plan.json` are identity-participating preimage artifacts. They MUST NOT embed the final package hash or concrete package-hash idempotency token because those values are derived from package identity. Runtime code reconstructs concrete `StateDelta` and destination commit inputs by combining the verified preimages with the finalized manifest package hash.

Canonical package data MUST be Arrow IPC file format with LZ4 framing. Stats, quarantine, and lineage SHOULD be Parquet. Manifests and receipts MUST be canonical JSON.

## Required layout

The package layout MUST preserve the book's directories and semantics: `manifest.json`, `plan/`, `schema/`, `data/`, `quarantine/`, `stats/`, `lineage/`, `state/`, `destination/`, and `trace.jsonl`.

`manifest.json` MUST list file path, byte count, and SHA-256 for every package file that participates in identity. It MUST carry lifecycle status and a reserved signature slot with defined signing input.

`destination/receipts.json` MUST remain outside identity because receipts are destination responses appended after destination interaction. State and commit-plan preimages are not receipt-like append-only responses; they are planned evidence and MUST participate in identity.

## Archive metadata and sidecar layout

`cdf package archive` MUST transcode canonical `data/` Arrow IPC segment files to Parquet sidecars while preserving Arrow IPC as the package's canonical data. The command MUST persist Parquet sidecars under `archive/parquet/data/<segment_id>.parquet` and MUST persist the fidelity report at `archive/parquet/fidelity.json`. Archive paths are package-relative, slash-separated, and derived from the existing manifest segment id.

The `archive/` directory is optional package metadata. It MUST NOT be part of the required identity layout, and archive files MUST NOT be added to `manifest.identity.files` or `manifest.identity.segments`.

When Parquet archive metadata exists, `manifest.json` MUST contain a top-level `archives.parquet` object outside `manifest.identity`:

- `format_version`: integer archive metadata version, initially `1`.
- `fidelity_report_path`: `archive/parquet/fidelity.json`.
- `fidelity_statement`: the same statement written to the fidelity report.
- `segments`: one entry per canonical data segment, sorted by manifest segment order.

Each `archives.parquet.segments[]` entry MUST contain `segment_id`, `source_path`, `source_byte_count`, `source_sha256`, `source_row_count`, `archive_path`, `archive_byte_count`, `archive_sha256`, and `archive_row_count`. The source fields MUST match the corresponding `manifest.identity.segments[]` entry. The archive path MUST match the sidecar path for that segment.

`archive/parquet/fidelity.json` MUST be canonical JSON and MUST contain `package_hash`, `source_format`, `archive_format`, `fidelity_statement`, and the same per-segment source/archive fields recorded in `manifest.archives.parquet.segments[]`. The fidelity statement MUST state that Arrow IPC remains canonical and that Parquet is an archive/interchange projection, not a promotion of Arrow-only semantics to canonical Parquet truth.

Archive metadata and archive files MUST NOT participate in `manifest.identity`, `package_hash`, receipt package hashes, idempotency tokens, or the existing signature signing input. Adding, replacing, or deleting Parquet sidecars MUST NOT change the package hash while canonical IPC identity files remain present. The manifest may be atomically rewritten to add or replace top-level archive metadata, but the signing input remains the hash of `manifest.identity` unless a later signing decision supersedes D-10.

Package verification MUST first verify canonical identity files. If `manifest.archives.parquet` is present, verification MUST also check that every recorded archive file exists, matches its byte count and SHA-256, and points back to unchanged source segment metadata. Archive verification failures MUST be reported distinctly from canonical identity failures; `cdf package verify` MUST exit nonzero when present archive metadata is false, even though replay from intact IPC remains possible.

## Lifecycle and crash matrix

Package lifecycle statuses MUST include planned, extracting, validated, packaged, loading, loaded, committed, checkpointed, and archived.

Recovery MUST follow the normative crash matrix:

- Before `packaged`, partial package is garbage unless resource can replay from recorded position.
- `packaged` with no receipts replays package to destination without source contact.
- Mid-load recovery uses transactional rollback or idempotent redrive keyed by token.
- Destination commit before checkpoint commit verifies receipt, then commits checkpoint without touching source.
- After checkpoint commit, the next run reads committed state.

Replay from a packaged artifact MUST derive concrete replay/checkpoint inputs from verified package identity evidence. The finalized package hash supplies the concrete `StateDelta.package_hash` and package-token idempotency value; those concrete values MUST be verified against receipts before crossing the commit gate.

Manifest status updates MUST be atomic rename-over or equivalently crash-safe.

`cdf package archive` MUST NOT change `lifecycle.status`. The `archived` lifecycle status is reserved for retention/GC tombstone behavior where canonical data may be absent and is not the status used for Parquet sidecar creation. Packages at `packaged`, `loaded`, `committed`, or `checkpointed` status MAY be archived. Packages at `planned`, `extracting`, `validated`, `loading`, or `archived` status MUST be refused by `cdf package archive`.

Replay MUST prefer canonical IPC segment paths from `manifest.identity.segments[]` whenever those IPC files are present. `cdf package archive` MUST NOT make replay read Parquet sidecars while IPC is present. Parquet-only replay requires a later active specification or decision.

## Retention and GC

Retention policy is per trust/environment. GC MUST refuse to collect any package that is sole proof of a committed checkpoint inside retention. GC MAY tombstone package data, but manifest and hashes MUST survive.

`cdf package archive` is fast-follow. Archive transcodes `data/` to Parquet with a fidelity report and updates package metadata without making Parquet canonical unless a later decision supersedes D-4.

Archive writes MUST be crash-safe. Implementations MUST write new Parquet files and `fidelity.json` into an operation-scoped temporary path below `archive/.tmp/`, verify byte counts and hashes there, install the final archive tree by atomic rename where the filesystem supports it, and atomically rewrite `manifest.json` last. A crash before the manifest rewrite MUST leave the previous manifest archive metadata in force. Stale temporary archive paths MUST be ignored by replay and verification, and a later archive run MAY remove them.

Archive reruns MUST be idempotent. If `manifest.archives.parquet` exists, all recorded archive files verify, and every recorded source field still matches `manifest.identity.segments[]`, `cdf package archive` MUST exit successfully without rewriting archive files and report `status = "skipped"`. If archive metadata or sidecar files are missing, mismatched, or orphaned under `archive/parquet/`, the default command MUST fail without changing the package. `--force` MAY replace the Parquet archive after canonical package verification succeeds; replacement MUST use the same temporary-write and manifest-last protocol.

The CLI contract is `cdf package archive <PACKAGE_DIR> [--format parquet] [--force] [--json]`. `parquet` is the only supported format until a later active spec adds another format. Human output MUST include package hash, archive status (`written`, `skipped`, or `replaced`), segment count, archive byte count, and fidelity report path. JSON output MUST include command, package hash, format, status, fidelity report path, fidelity statement, and the per-segment fields from the manifest archive metadata. Successful writes, replacements, and idempotent skips exit `0`. Unsupported formats, failed canonical package verification, unsafe existing archive state without `--force`, unsupported IPC-to-Parquet projection, IO failure, or partial-write failure exit nonzero.

## Determinism

Golden-package tests MUST prove stable manifest hashes for fixed fixtures across repetitions and operating systems. Canonical JSON, deterministic test ULIDs, recorded adaptive batch sizes, and enumerated unhashed wall-clock fields are required mechanisms.

Upstream dependency upgrades that alter IPC bytes, manifest hash, or plan text MUST be caught by golden-package release gates.

## Acceptance criteria

- A fixed source fixture produces identical package manifest hash across repeated test runs.
- Package identity includes state input, state-delta preimage, and destination commit-plan preimage artifacts without a package-hash cycle.
- Recovery tests cover every lifecycle boundary in the crash matrix.
- Package verification detects tampered files by hash.
- GC leaves tombstones for collected committed packages and preserves ledger referential integrity.
- `cdf package archive` persists Parquet sidecars and fidelity metadata without changing `manifest.identity`, `package_hash`, lifecycle status, receipt package hashes, or IPC replay preference.
- Archive verification detects tampered, missing, or source-mismatched archive sidecars when archive metadata is present.
- Archive reruns skip clean archives, require `--force` for replacement, and leave canonical packages replayable after interrupted writes.

## Explicit exclusions

This spec does not define the checkpoint database schema, destination receipt contents beyond package storage, resource authoring APIs, Parquet-only replay, archive deletion, or a future archive format beyond Parquet.
