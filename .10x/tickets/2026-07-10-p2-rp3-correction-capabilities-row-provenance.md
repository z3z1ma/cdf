Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/specs/schema-promotion-corrections.md, .10x/specs/destination-receipts-guarantees.md

# P2 RP3 destination correction capabilities and row provenance

## Scope

Add backward-compatible kernel destination-sheet vocabulary for persisted/targetable row provenance, residual readback, correction strategies, and their transaction/idempotency claims. Define the logical `(package, segment, ordinal)` address plus executor-neutral correction request/plan values, and reconcile existing Postgres system columns plus DuckDB/Parquet declarations.

## Acceptance criteria

- Serialized sheets gain additive defaulted correction capabilities without breaking legacy sheet/lock fixtures.
- Row provenance is one kernel type using original package hash, segment id, and zero-based row ordinal.
- Postgres declares current provenance persistence only after uniqueness/targetability is proven; DuckDB and Parquet do not overclaim.
- Strategy values are exactly `in_place_update`, `correction_sidecar`, and `versioned_rematerialization`, each with transaction/idempotency evidence.
- Correction request/plan values carry promotion id, original row address, old/new schema hashes, promoted path/value, residual operation, and selected strategy without importing a destination driver or CLI type.
- Planner-facing validation rejects impossible combinations such as in-place update without targetable provenance.
- Destination conformance can falsify every new claim.

## Evidence expectations

Semver/serialization fixtures, lockfile sheet snapshots, Postgres provenance inspection, negative capability validation, and kernel/destination conformance scaffolding.

## Explicit exclusions

No actual correction write, readback implementation, promotion planner, or lockfile publication.

## Progress and notes

- 2026-07-10: Source audit found Postgres already writes `_cdf_load` from the package idempotency token plus `_cdf_segment` and segment-local `_cdf_row`; this ticket must reuse that tuple.

## Blockers

None.
