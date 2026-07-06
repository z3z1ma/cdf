Status: open
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-datafusion-engine-planner.md, .10x/tickets/done/2026-07-05-duckdb-destination.md, .10x/tickets/done/2026-07-05-parquet-object-store-destination.md, .10x/tickets/2026-07-05-postgres-destination.md

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
- 2026-07-06: Split child `.10x/tickets/done/2026-07-06-checkpoint-store-conformance-suite.md` for the first reusable conformance harness over the public checkpoint-store contract. This advances store conformance independently of unfinished resource, destination, chaos, and golden-package suites.
- 2026-07-06: Closed checkpoint-store conformance child with mutation-hardened reusable harness, MVP in-memory/SQLite store integration, and evidence/review records. Resource conformance, destination conformance, chaos, golden-package fixtures, and MVP killer-demo harness remain open in this parent.
- 2026-07-06: Split child now closed at `.10x/tickets/done/2026-07-06-destination-conformance-suite-foundation.md` for the first reusable destination sheet/planning conformance harness and DuckDB/Parquet consumer tests. Live Postgres conformance remains excluded until `.10x/tickets/2026-07-05-postgres-destination.md` is unblocked.
- 2026-07-06: Closed destination foundation child as `.10x/tickets/done/2026-07-06-destination-conformance-suite-foundation.md` with reusable sheet/planning harness, DuckDB/Parquet consumer tests, mutation evidence, and review. Resource conformance, chaos killpoints, golden-package fixtures, MVP killer-demo harness, and live Postgres destination conformance remain outside this closed child.
- 2026-07-06: Split and closed child `.10x/tickets/done/2026-07-06-resource-conformance-suite-foundation.md` for the first reusable planning-level resource conformance harness over public `ResourceStream`/`QueryableResource` contracts, with declarative REST/SQL/file consumers. Evidence is recorded in `.10x/evidence/2026-07-06-resource-conformance-suite-foundation.md`; review passed in `.10x/reviews/2026-07-06-resource-conformance-suite-foundation-review.md`. Resource source execution, data completeness, position replay, chaos, golden packages, MVP demo, and live Postgres conformance remain open parent scope.

## Blockers

None.
