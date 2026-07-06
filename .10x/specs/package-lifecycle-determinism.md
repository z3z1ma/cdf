Status: active
Created: 2026-07-05
Updated: 2026-07-05

# Packages, lifecycle, and determinism

## Purpose and scope

This specification governs load package layout, lifecycle, crash recovery semantics at package boundaries, retention, hashing, signature readiness, archive behavior, and golden-package determinism. It derives from book Chapters 11 and 19 and decisions D-4, D-10, D-13, D-21, and D-28.

## Package meaning

A load package is the durable, hash-addressed evidence of one attempted state transition for one resource-partition set. Staging is a side effect; evidence is the identity.

Packages MUST contain or reference the plan, observed/output schemas, contract decisions, normalized data, quarantine, stats, lineage, input checkpoint, proposed state delta, destination commit plan, receipts, and trace.

Canonical package data MUST be Arrow IPC file format with LZ4 framing. Stats, quarantine, and lineage SHOULD be Parquet. Manifests and receipts MUST be canonical JSON.

## Required layout

The package layout MUST preserve the book's directories and semantics: `manifest.json`, `plan/`, `schema/`, `data/`, `quarantine/`, `stats/`, `lineage/`, `state/`, `destination/`, and `trace.jsonl`.

`manifest.json` MUST list file path, byte count, and SHA-256 for every package file that participates in identity. It MUST carry lifecycle status and a reserved signature slot with defined signing input.

## Lifecycle and crash matrix

Package lifecycle statuses MUST include planned, extracting, validated, packaged, loading, loaded, committed, checkpointed, and archived.

Recovery MUST follow the normative crash matrix:

- Before `packaged`, partial package is garbage unless resource can replay from recorded position.
- `packaged` with no receipts replays package to destination without source contact.
- Mid-load recovery uses transactional rollback or idempotent redrive keyed by token.
- Destination commit before checkpoint commit verifies receipt, then commits checkpoint without touching source.
- After checkpoint commit, the next run reads committed state.

Manifest status updates MUST be atomic rename-over or equivalently crash-safe.

## Retention and GC

Retention policy is per trust/environment. GC MUST refuse to collect any package that is sole proof of a committed checkpoint inside retention. GC MAY tombstone package data, but manifest and hashes MUST survive.

`firn package archive` is fast-follow. Archive transcodes `data/` to Parquet with a fidelity report and updates package metadata without making Parquet canonical unless a later decision supersedes D-4.

## Determinism

Golden-package tests MUST prove stable manifest hashes for fixed fixtures across repetitions and operating systems. Canonical JSON, deterministic test ULIDs, recorded adaptive batch sizes, and enumerated unhashed wall-clock fields are required mechanisms.

Upstream dependency upgrades that alter IPC bytes, manifest hash, or plan text MUST be caught by golden-package release gates.

## Acceptance criteria

- A fixed source fixture produces identical package manifest hash across repeated test runs.
- Recovery tests cover every lifecycle boundary in the crash matrix.
- Package verification detects tampered files by hash.
- GC leaves tombstones for collected committed packages and preserves ledger referential integrity.

## Explicit exclusions

This spec does not define the checkpoint database schema, destination receipt contents beyond package storage, or resource authoring APIs.

