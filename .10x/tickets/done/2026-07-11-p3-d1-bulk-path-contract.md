Status: done
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/done/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/tickets/done/2026-07-11-p3-a1-staged-ingress-final-binding.md, .10x/tickets/done/2026-07-11-p3-a5a-graph-edge-contracts.md, .10x/specs/destination-bulk-path-runtime.md

# P3 D1: neutral bulk-path descriptors and bounded writer contract

## Scope

Implement neutral bulk descriptors/preparation, schema eligibility/rejection, bounded segment/batch writer input, preflight and rollback/redrive fallback, tuning join, receipt/run physical evidence, and generic conformance. Migrate first-party sheet strings/enums through compatibility descriptors without optimizing drivers.

## Acceptance criteria

- A mock destination declares two paths/fallback/staging/concurrency without generic runtime changes.
- Sheet declarations are live-falsified and package-sized segment/row collections cannot cross the production writer API.
- Physical choice/settings remain outside package identity but are durably auditable.
- Forced mid-attempt fallback requires proven abort/new attempt/full redrive.
- Existing first-party semantic receipts/gate behavior remains stable through adapters.

## Evidence expectations

Serialization/artifact invariance, mock-driver conformance, architecture/static gates, fallback/crash matrix, memory/cancellation, receipt compatibility, and dispatch overhead benchmark.

## Explicit exclusions

No DuckDB/Postgres/Parquet optimization.

## Blockers

None. L5, DX1, staged ingress, graph-edge ownership, and the verified accounted segment reader are implemented. Depending on the A5 parent would be circular because A5c needs D1/D2 to remove DuckDB materialization.

## Progress and notes

- 2026-07-11: Added versioned neutral descriptors, preparation/rejection, accounted batch iteration, and first-party compatibility declarations in `81281a77`.
- 2026-07-11: Added proof-gated rollback/full-redrive coordination with distinct attempt identities in `1bfab8ca`.
- 2026-07-11: Added validated serializable physical-attempt run evidence in `f72db7a0`.
- 2026-07-11: Acceptance is supported by `.10x/evidence/2026-07-11-p3-d1-bulk-path-contract.md`; adversarial review passed in `.10x/reviews/2026-07-11-p3-d1-bulk-path-contract-review.md`.

## References

- `.10x/decisions/schema-planned-destination-bulk-paths.md`
- `.10x/research/2026-07-11-destination-bulk-path-audit.md`
- `.10x/specs/destination-bulk-path-runtime.md`
