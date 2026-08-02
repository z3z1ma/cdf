Status: active
Created: 2026-08-02
Updated: 2026-08-02

# SQLite destination

## Purpose and scope

This specification governs a first-party SQLite destination with append, atomic replace, and
merge. It is further governed by `.10x/specs/destination-extension-runtime-contract.md`,
`.10x/specs/destination-receipts-guarantees.md`,
`.10x/specs/destination-common-services.md`, and
`.10x/specs/destination-bulk-path-runtime.md`.

## Destination contract

`cdf-dest-sqlite` MUST be distinct from `cdf-state-sqlite` and resolve `sqlite://` destination
locations plus a validated target table. It implements finalized-package ingress with one
run-owned `rusqlite` connection on one declared blocking lane. Its sheet MUST advertise one writer,
atomic package transactions, package-token idempotency, migration support, and append/replace/merge.

One explicit transaction MUST cover target DDL/DML, compact lossless row provenance, `_cdf_loads`,
`_cdf_state`, `_cdf_segments`, and quarantine/mirror mutations. The adapter MUST implement the
typed `cdf-dest-sql` mirror backend instead of reconstructing the lifecycle. A duplicate verified
package token returns the same logical receipt without rewriting rows.

Append inserts every package row. Replace deletes and inserts inside the same transaction and is
therefore invisible until commit. Merge requires nonempty normalized merge keys, performs
deterministic package dedup before mutation, and updates/inserts by those keys. There is no
delete-then-insert period visible outside the transaction.

The connector MUST respect an existing database's journal and synchronous settings. It MUST NOT
silently enable WAL or reduce durability. An explicit future/operator setting may request WAL,
but a failed or unsupported request fails preflight rather than changing semantics silently.

## Mapping and bulk path

The default bulk path is reused prepared statements over Arrow batches inside the package
transaction. Statement and column binding are prepared once; rows are converted directly from
Arrow arrays without intermediate row maps or JSON objects.

The sheet MUST declare at least these lossless representations: booleans and signed integers as
INTEGER; `UInt8`/`UInt16`/`UInt32` as widening INTEGER; `UInt64` and decimals as canonical decimal
TEXT; floating values as REAL; UTF-8 as TEXT; binary as BLOB; dates, times, durations, intervals,
and timestamps as canonical integer units with the pinned Arrow unit/timezone retained in CDF
schema evidence. Float16 widens to REAL. Nested, union, dictionary, run-end encoded, and values
whose domain cannot be represented exactly MUST fail planning unless the existing contract grants
a declared lossy/canonical-JSON mapping. There is no silent stringification.

Identifiers use the shared normalized-name authority plus SQLite quoting. `_cdf_*` is reserved.
Existing target types and nullability MUST be inspected before mutation; incompatible mappings
fail with field-level remediation.

## Receipts and verification

The package hash is the idempotency token. Receipt transaction metadata MUST identify the SQLite
transaction/connection evidence available without exposing paths. Independent verification MUST
read the load, segment, state, schema, count, and provenance mirror facts from a fresh connection.
Only generic orchestration appends the receipt to the package and advances the checkpoint.

## Scenarios and acceptance criteria

- A crash before commit leaves no target or mirror rows; a crash after commit but before checkpoint
  is recovered by independent receipt verification.
- Replaying append, replace, or merge with the same package token is a verified no-op.
- Replace is never externally observable as an empty target.
- Duplicate merge keys fail according to deterministic `fail` policy before target mutation.
- Existing journal/durability modes remain unchanged after success and failure.
- All destination conformance, crash, receipt, jobs-invariance, and connector-certification laws
  pass, and the destination macro cell meets the 0.90 direct-`rusqlite` roofline.

## Explicit exclusions

Concurrent writers, network-filesystem guarantees, silent WAL activation, arbitrary user SQL,
resident CDC application, and cross-database atomicity across attached SQLite files are excluded.
