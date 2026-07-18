Status: active
Created: 2026-07-18
Updated: 2026-07-18

# Canonical package row ordinal

## Purpose and scope

This specification governs the internal package-global ordinal used to bridge canonical CDF rows into compact destination provenance. Public logical provenance remains governed by `.10x/decisions/compact-lossless-destination-row-provenance.md`; destination-visible output schemas remain governed by `.10x/decisions/compiled-output-schema-and-runtime-provenance.md`.

## Assignment and schema

After filtering, validation, quarantine routing, residual materialization, normalization, and package-scoped dedup, canonical assembly MUST assign every admitted output row a dense zero-based package ordinal. Jobs, pressure, spill, encode completion order, destination speed, and replay timing MUST NOT change it.

The canonical segment storage schema MUST equal the compiled logical output schema followed by exactly one field:

```text
name: _cdf_package_row_ordinal
type: uint64
nullable: false
metadata:
  cdf:semantic: package-row-ordinal-v1
  cdf:visibility: internal
```

The logical output schema MUST exclude this field. A logical schema containing the reserved name or an internal field with a non-exact contract MUST fail before package mutation.

## Segment and manifest contract

Manifest segment entries MUST record `package_row_ordinal_start`. In canonical manifest order, the first segment starts at zero and every next start equals the previous start plus previous row count. Every stored batch value MUST be consecutive across batch boundaries, and the final value in a nonempty segment MUST equal `start + row_count - 1`.

Package verification and replay MUST reject missing, nullable, wrong-type, wrong-metadata, non-dense, duplicated, decreasing, out-of-range, or manifest-disagreeing ordinal evidence before destination mutation. The current package manifest/storage version MUST reject pre-ordinal artifacts; there is no compatibility reader or migration shim.

## Destination contract

A relational destination that persists compact row keys MUST reserve one package-sized key range transactionally and compute each payload key as:

```text
allocated_package_start + _cdf_package_row_ordinal
```

For every segment, its physical range starts at `allocated_package_start + package_row_ordinal_start`. Subtracting that range start yields the public zero-based segment row ordinal. Destinations MUST exclude `_cdf_package_row_ordinal` from user-visible target columns unless an explicit physical/explain surface requests it.

Merge staging MAY use `_cdf_package_row_ordinal` as deterministic package order and MUST NOT generate a second stage-order sequence. File destinations MAY strip the internal field while preserving the manifest-bound `(object, segment offset, row ordinal)` mapping.

## Accounting and performance

Ordinal construction MUST allocate at most one contiguous `UInt64` value buffer per canonical output batch and MUST be charged to the shared memory ledger before allocation. It MUST NOT loop through destination APIs or add per-row synchronization. Statistics over user data exclude the internal field unless an explicit physical profile is requested.

Retention requires controlled before/after evidence for canonical package encode bytes/time/RSS plus DuckDB, Postgres, and Parquet destination paths. The jobs-invariance golden law MUST include ordinal values and package hashes. A first-party destination may not retain a slower ordinal-consumption strategy when an exact faster path is available.

## Scenarios

Given canonical segments with row counts 2 and 3, when they are emitted, then their recorded starts are 0 and 2 and stored ordinals are `[0,1]` and `[2,3,4]`.

Given the same run under jobs 1 and N with different encode completion order, when packages finalize, then ordinal arrays, segment starts, segment hashes, and package hash are identical.

Given an ordinal-tampered verified segment, when replay prepares destination ingress, then verification fails before the destination mutation guard is crossed.

Given an allocated relational range beginning at 100 and a segment start of 2, when that segment is committed, then its row-key range begins at 102 and logical row ordinal zero resolves from key 102.

## Acceptance criteria

- Exact internal-field construction/classification and manifest continuity tests pass.
- Package read, replay, live staged ingress, and artifact replay use the same persisted ordinal authority.
- DuckDB and Postgres no longer generate destination-local row enumeration for eligible bulk paths.
- Parquet visible schemas remain free of the internal field while provenance lookup remains exact.
- Jobs invariance, duplicate replay, rollback, append, replace, merge, correction readback, and receipt verification remain green.
- Controlled performance evidence records the byte/time/RSS delta and full-CDF destination throughput.

## Explicit exclusions

This field is not a user key, merge key, cursor, source row ordinal, public row address, or destination-global key. It does not permit schema evolution during a run and does not weaken package verification.

