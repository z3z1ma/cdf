Status: done
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/done/2026-07-05-package-builder-reader.md

# Implement Parquet/object-store destination

## Scope

Implement `cdf-dest-parquet`: local filesystem and object_store-backed Parquet materialization, partition layout, manifest receipts with key/etag/sha256, append/replace semantics where supported, package-token idempotency by object keys/manifests, and lakehouse seam metadata. Owns `crates/cdf-dest-parquet/**`.

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
- 2026-07-06: Assigned to Parquet/object-store destination worker after re-reading book Chapters 13, 19, and 22, `.10x/specs/destination-receipts-guarantees.md`, `.10x/specs/package-lifecycle-determinism.md`, and existing package/DuckDB destination APIs. Worker owns `crates/cdf-dest-parquet/**`, necessary direct dependency declarations for that crate, and its own evidence/review records. Leave `.gitignore`, Postgres, DuckDB, parent tickets, and unrelated records untouched.
- 2026-07-06: Implemented the `cdf-dest-parquet` local filesystem/object_store Parquet destination with append and replace support, package-token idempotency, dry-run planning, object manifest receipts, package receipt recording, and receipt verification. Added focused tests for filesystem append/readback, in-memory object_store duplicate replay, replace pointer behavior, dry-run no-write behavior, tamper/missing-object verification failure, sheet truth, and requested-segment validation. Required checks passed and are recorded in `.10x/evidence/2026-07-06-parquet-object-store-destination-verification.md`; review recorded in `.10x/reviews/2026-07-06-parquet-object-store-destination-review.md`. Ready for parent review.
- 2026-07-06: Parent review found and resolved two issues before closure: receipt object manifests now distinguish state byte counts from package IPC bytes and written Parquet bytes; the writer no longer depends on arrow-rs `parquet`/`paste`, using DuckDB Parquet export instead. Added mutation-focused tests for duplicate replay planning, replace-pointer identity, manifest/receipt identity, duplicate column names, canonical JSON arrays, key derivation, prefix normalization, and timestamp plausibility. Final verification is recorded in `.10x/evidence/2026-07-06-parquet-object-store-destination-verification.md`: targeted and workspace tests/clippy passed, nextest passed, advisory scanners passed, targeted Semgrep and source-only gitleaks passed, CodeQL completed with zero extraction errors, and mutation testing finished with 158 mutants tested, 128 caught, 30 unviable, 0 missed.

## Blockers

None.
