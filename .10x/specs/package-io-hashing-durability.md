Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Package I/O, hashing, and durability

## Purpose and scope

This specification governs artifact sinks/receipts, hash-while-write, draft indexes, durability barriers, streaming manifest finalization/verification, trace buffering, and replay read evaluation. Package semantic layout/lifecycle remains governed by `.10x/specs/package-lifecycle-determinism.md`.

## Artifact sink

Every identity file writer MUST produce exactly one typed receipt from exact bytes sent to the installed file. Encoders with internal buffers MUST finish before the digest/count finalize. Partial/failed writes yield no receipt and cannot enter draft indexes.

Temp creation is exclusive; final path normalization and duplicate checks occur before write. File sync precedes rename whenever the durability class requires persistence. Directory errors fail before publication authority is returned. Receipts include path/count/SHA-256/durability, and segment receipts additionally bind segment id/rows/schema identity.

Hash calculation MUST occur in the write call chain without copying the complete payload. In-memory small artifacts MAY hash the existing byte slice once. Hash state/buffers are accounted. SHA implementation/version/features and measured GiB/s are lab evidence.

## Draft index and finalization

File and segment receipts MUST enter bounded append/spill-backed draft indexes. Duplicate paths/segment ids, missing registered files, unexpected identity files, non-files, symlinks, path escapes, size changes, or unsatisfied durability barriers fail finalization.

Canonical sort/serialization MUST be streaming and deterministic. Manifest v1 output for existing fixtures MUST remain byte/hash identical. Package hash is the incremental SHA-256 of canonical `ManifestIdentity`. The outer manifest is atomically streamed using the computed package hash without storing complete identity/manifest bytes in memory.

Finalization MUST NOT reopen registered content solely to recompute its receipt. Compatibility hashing is at most once per unregistered file and cannot remain on the steady-state production path at closure.

## Durability protocol

Segment-publish completion requires encoder finish, file sync, rename, and containing-directory sync. Only then may the graph emit a durable segment or stage it.

Phase metadata may batch directory barriers, but every referenced directory is synced before final manifest. The final manifest temp is fully written/synced, renamed last, and package directory synced. A crash before manifest publication leaves a non-replayable draft; after publication, every referenced identity entry is durable under the supported filesystem contract.

Trace writes use one bounded append sink per builder/run. Required ordering is semantic event order, independent of task completion; the runtime reorder/evidence sink supplies it. Trace flush/sync occurs at phase boundaries and before final manifest, not per event. Receipts remain outside identity and retain their existing durable append/atomic contract.

## Reading and verification

Production segment readers MUST stream batches and hash exact file bytes during the same consumer read where technically possible. Hash mismatch fails before segment acknowledgement/final binding. A separate full read is used only for explicit `cdf package verify` or when no consuming read exists and policy requires independent verification.

Package verification MUST stream manifest entries and file hashes with bounded worker concurrency under memory/I/O admission. Report ordering is canonical even when hashing runs in parallel. It detects unexpected/missing/tampered files and segment/file mismatch without resident package-sized maps.

Mmap is optional and local-only. It cannot be introduced without the unsafe decision/fuzz/safety gate and evidence that it improves the relevant replay path after page-fault/cache effects are labeled.

## Performance and conformance

Permanent tests MUST inject failure before/after write, encoder finish, file sync, rename, directory sync, draft receipt, metadata barrier, identity hash, manifest sync/rename, and status/receipt updates. Recovery/visibility must match the crash matrix.

Goldens MUST prove unchanged v1 bytes. High-cardinality tests MUST finalize/verify with bounded memory. The lab records logical/physical bytes, reread bytes, hash/encode/write/sync/manifest/verify durations, SHA rate, syscall counts, and device roofline. Package build must meet ≥70% device write roofline and hashing ≤5% wall.

## Explicit exclusions

This spec does not change SHA-256, canonical IPC/LZ4, package layout, signature input, lifecycle meaning, or authorize unsafe mmap.
