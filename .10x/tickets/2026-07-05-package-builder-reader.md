Status: open
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md

# Implement package builder, reader, hasher, verifier, and replayer core

## Scope

Implement package manifest models, canonical JSON hashing, package directory lifecycle, Arrow IPC segment writing/reading, stats/quarantine/lineage artifact hooks, receipt appending, package verification, tombstoning, and replay views. Owns `crates/firn-package/**`.

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

## Blockers

None.

