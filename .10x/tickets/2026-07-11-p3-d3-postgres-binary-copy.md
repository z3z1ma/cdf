Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-11-p3-d1-bulk-path-contract.md, .10x/tickets/done/2026-07-11-p3-a1-staged-ingress-final-binding.md

# P3 D3: Arrow-to-Postgres binary COPY writer

## Scope

Implement vectorized PostgreSQL binary COPY encoding from Arrow into bounded stage/attempt ingestion, retain truthful CSV compatibility, eliminate scalar string row collections, and preserve transactional disposition/receipt semantics.

## Acceptance criteria

- Exact supported type matrix uses binary COPY with correct endian/epoch/decimal/array/null framing.
- Local throughput is ≥2x CSV baseline and remote profile is network/server-bound where expected.
- Stage/transaction lifecycle is bounded, invisible before final binding, rollback-safe, and duplicate-idempotent.
- CSV fallback is schema-preplanned, redacted, and semantically identical or rejected when it cannot be.

## Evidence expectations

Protocol/type golden vectors, PostgreSQL round trips, binary-vs-CSV benchmark, allocation/network profiles, transaction/crash/replay/correction conformance, and receipt verification.

## Explicit exclusions

No destination-generic PostgreSQL type assumptions.

## Blockers

Depends on D1 and staged-ingress contract.

## Progress and notes

- 2026-07-11: Removed the CSV compatibility path's package/segment scalar-row materialization. `PostgresPackageData` now retains bounded Arrow batches for the current verified segment; CSV fields are encoded directly into COPY one row at a time. All 30 unit/live transaction, merge, correction, rollback, receipt, and source tests remain green. This establishes the constant-memory encoder boundary that binary COPY will replace without changing commit semantics.

## References

- `.10x/specs/destination-bulk-path-runtime.md`
