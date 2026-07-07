Status: done
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md

# Implement package builder, reader, hasher, verifier, and replayer core

## Scope

Implement package manifest models, canonical JSON hashing, package directory lifecycle, Arrow IPC segment writing/reading, stats/quarantine/lineage artifact hooks, receipt appending, package verification, tombstoning, and replay views. Owns `crates/cdf-package/**`.

## Acceptance criteria

- Package layout matches `.10x/specs/package-lifecycle-determinism.md`.
- Manifest identity is SHA-256 over canonicalized identity-participating data.
- Lifecycle status updates are crash-safe by atomic rename-over or equivalent.
- Fixed fixtures produce deterministic manifest hashes across repeated runs.
- Verification detects file tampering.

## Evidence expectations

Record package unit tests, golden-hash tests, tamper tests, and lifecycle update tests.

## Explicit exclusions

No destination-specific commit, no DataFusion execution, no package archive until the fast-follow ticket.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-06: Assigned to package worker after checkpoint-store closure. Worker owns `crates/cdf-package/**` and may propose minimal `cdf-kernel` additions only when required by `.10x/specs/package-lifecycle-determinism.md`; leave unrelated dirty `.gitignore` changes untouched.
- 2026-07-06: Implemented package manifest models, canonical JSON identity hashing, required directory layout, LZ4 Arrow IPC segment write/read, artifact hooks, receipt append/storage, verification, atomic lifecycle status updates, tombstoning, replay views, and focused package tests in `crates/cdf-package`. No `cdf-kernel` additions were required. Evidence recorded in `.10x/evidence/2026-07-06-package-builder-reader.md`.
- 2026-07-06: Parent review confirmed receipts are stored outside identity and replay-visible, package verification catches tampering, and tombstoning preserves manifest/hash records. Closure evidence recorded in `.10x/evidence/2026-07-06-package-contract-http-quality-gates.md`; closure review recorded in `.10x/reviews/2026-07-06-package-builder-reader-review.md`.

## Blockers

None.
