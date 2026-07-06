Status: open
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/2026-07-05-package-builder-reader.md

# Implement Parquet/object-store destination

## Scope

Implement `firn-dest-parquet`: local filesystem and object_store-backed Parquet materialization, partition layout, manifest receipts with key/etag/sha256, append/replace semantics where supported, package-token idempotency by object keys/manifests, and lakehouse seam metadata. Owns `crates/firn-dest-parquet/**`.

## Acceptance criteria

- Package segments can materialize as Parquet with declared fidelity.
- Receipt verification checks object keys, etags where available, hashes, counts, and schema hash.
- Re-driving the same package is safe under package-token layout.
- Destination sheet declares unsupported semantics honestly.
- Object-store and filesystem paths share the same commit protocol.

## Evidence expectations

Record filesystem integration tests, mocked object_store tests, receipt tamper tests, replay tests, and sheet conformance fixtures.

## Explicit exclusions

Iceberg and Delta are post-MVP and owned by `.10x/tickets/2026-07-05-lakehouse-warehouse-and-vault.md`.

## Progress and notes

- 2026-07-05: Opened from book and specs.

## Blockers

None.

