Status: open
Created: 2026-07-05
Updated: 2026-07-05
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-datafusion-engine-planner.md, .10x/tickets/done/2026-07-05-duckdb-destination.md, .10x/tickets/2026-07-05-parquet-object-store-destination.md, .10x/tickets/2026-07-05-postgres-destination.md

# Implement conformance suites, chaos layer, and golden packages

## Scope

Implement `firn-conformance`: resource suite, destination suite, checkpoint store conformance, chaos killpoints for lifecycle boundaries, golden-package fixtures, property/fuzz targets, and MVP killer-demo harness. Owns `crates/firn-conformance/**`, test fixtures, and CI hooks.

## Acceptance criteria

- Resource conformance tests descriptors, capability truth, partition completeness, position replay, and boundedness.
- Destination conformance tests sheet truth, dispositions, migrations, idempotency, receipt verification, replay identity, and exact type mappings.
- Chaos layer kills every lifecycle boundary and proves no cursor advances ahead of durable data.
- Golden fixtures compare package evidence hash-by-hash.
- MVP killer demo can run under the conditions described by the governance spec.

## Evidence expectations

Record conformance suite output, chaos run output, golden package hashes, and MVP killer-demo evidence.

## Explicit exclusions

No new production behavior except test hooks required for chaos.

## Progress and notes

- 2026-07-05: Opened from book and specs.

## Blockers

None.
