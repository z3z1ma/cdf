Status: active
Created: 2026-08-02
Updated: 2026-08-02

# MongoDB destination

## Purpose and scope

This specification governs a first-party MongoDB 8.0+ destination with append, atomic replace,
and merge. It is further governed by `.10x/specs/destination-extension-runtime-contract.md`,
`.10x/specs/destination-receipts-guarantees.md`, and
`.10x/specs/destination-bulk-path-runtime.md`.

## Destination contract

`cdf-dest-mongodb` MUST be a leaf adapter resolving `mongodb`/`mongodb+srv` schemes, secret-backed
credentials, and a validated database/collection target. The deployment MUST support multi-document
transactions; standalone servers fail destination preflight with replica-set/sharded remediation.
The sheet advertises MongoDB 8.0 or later, atomic package transactions, package-token idempotency,
one logical transaction writer, and append/replace/merge.

One session transaction MUST cover payload writes plus connector-owned load, segment, state,
schema, provenance, and quarantine collections. Transaction retry and ambiguous commit handling
MUST use the official driver's session/transaction protocol under injected cancellation and retry
budgets. The adapter cannot implement its own checkpoint or claim success before committed receipt
facts are independently readable.

Append performs batched inserts. Replace deletes the prior target and inserts the complete package
inside the same transaction, so readers see the old or new committed collection state. Merge
requires nonempty normalized merge keys, deterministic package dedup before mutation, and batched
replace/upsert models keyed by the canonical merge tuple. No update operator may preserve stale
fields accidentally; merge replaces the destination document represented by the Arrow row.

## Deterministic document identity

MongoDB `_id` is CDF-owned destination identity in this connector and is reserved from normalized
input fields. An input field that normalizes to `_id` MUST be explicitly renamed before planning.

- Append and replace derive `_id` as BSON binary from a versioned SHA-256 preimage containing the
  destination, target, package hash, canonical segment id, and row ordinal.
- Merge derives `_id` from destination, target, and the canonical typed merge-key tuple so later
  packages address the same logical document.

The derivation version is destination sheet/plan authority. It MUST be collision-tested, stable
across jobs/replay, and independently recomputable during verification. The driver MUST never let
MongoDB generate random ObjectIds for CDF package rows.

## Mapping and bulk path

The production path converts Arrow arrays directly into bounded raw BSON documents and submits
official driver `insert_many` or `Client::bulk_write` operations. It reuses one `Client`/pool;
document maps, Extended JSON, per-row network calls, and synchronous wrapper loops are forbidden
bulk fallbacks.

The sheet MUST map bool, signed integers, `UInt8`/`UInt16`/`UInt32` widening to Int64, Float64,
UTF-8, binary, millisecond UTC timestamps, Decimal128 within BSON precision, lists, and structs/maps
with valid string keys. Float16/Float32 may widen to Double. Finer-than-millisecond timestamps,
UInt64 outside signed/Decimal128 exact domains, Decimal256, unions, duplicate keys, run-end encoded
arrays, non-string map keys, and unsupported extension values fail planning unless a declared
contract allowance selects an explicit lossless envelope or lossy mapping. Date-only, time-only,
duration, interval, timezone, and source semantic annotations MUST remain in connector schema
evidence; there is no silent local-time or Extended JSON conversion.

Normalized field names follow shared authority plus MongoDB field restrictions; `_cdf_*` and `_id`
are reserved. Existing validators, unique indexes, shard keys, and target schema are inspected
before mutation. Merge keys MUST be compatible with shard routing and existing uniqueness or fail
preflight.

## Receipts, execution, and performance

The package hash is the idempotency token. A duplicate transaction reads the existing load fact and
returns the same logical receipt without payload mutation. Independent verification from a fresh
session checks load/segment facts, deterministic `_id` derivation, counts, schema, state, and target
visibility. Generic orchestration alone records package receipt evidence and advances checkpoints.

Bulk batches, pool bounds, and in-flight operations are selected from measured capabilities and
host memory/CPU pressure. Operations within one transaction obey official driver session
serialization; the adapter MUST NOT launch unsupported concurrent session operations merely to
inflate utilization. Upstream BSON encoding may overlap only within injected bounded tasks.

The direct-library roofline follows `.10x/specs/database-connector-roofline.md`.

## Scenarios and acceptance criteria

- Crash/ambiguous-commit tests at every transaction boundary leave either no package or one
  independently verifiable package and never advance the checkpoint early.
- Replaying append, replace, or merge with the same token is a no-op with identical logical receipt.
- Replace readers observe complete old or complete new state, including zero-row replacement.
- Merge dedup and deterministic `_id` remain invariant across segmenting and jobs settings.
- Nested BSON round trips preserve supported Arrow values and rejected/lossy mappings fail at plan
  time with field-level remediation.
- Destination inspection/planning/health, receipt/replay/crash, redaction, jobs invariance, live
  MongoDB 8.0+ conformance, and connector certification pass.
- The destination macro cell meets the 0.90 official-driver direct-library roofline.

## Explicit exclusions

Standalone MongoDB, pre-8.0 compatibility, random ObjectIds, change-stream application, patch-style
merge, unbounded transactions/concurrency, arbitrary aggregation writes, and silent Extended JSON
fallback are excluded.
