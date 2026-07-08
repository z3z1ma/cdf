Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-07-p0-workstream-c-spine-conformance-harness.md, .10x/tickets/done/2026-07-08-p0-c6-workstream-c-closure.md

# P0 Workstream C spine conformance harness evidence

## What was observed

P0 Workstream C is closed through five executable children plus this aggregate closure:

- C1 matrix foundation: `.10x/tickets/done/2026-07-08-p0-c1-run-spine-matrix-foundation.md`
- C2 REST/SQL run matrix: `.10x/tickets/done/2026-07-08-p0-c2-rest-sql-run-matrix.md`
- C3 cross-destination chaos: `.10x/tickets/done/2026-07-08-p0-c3-cross-destination-chaos.md`
- C4 live-run goldens per destination: `.10x/tickets/done/2026-07-08-p0-c4-live-run-goldens-per-destination.md`
- C5 property/fuzz targets: `.10x/tickets/done/2026-07-08-p0-c5-property-fuzz-targets.md`

Aggregate Workstream C coverage now includes:

- FILE, deterministic REST fixture, and table-backed Postgres SQL source archetypes through `cdf_project::run_project`.
- DuckDB, filesystem Parquet, and Postgres destinations.
- Append, replace, and merge where destination sheets support them.
- 24 executed run-matrix cells and 3 explicit Parquet merge exclusions.
- Generic runtime chaos for DuckDB, filesystem Parquet, and Postgres across all four ratified crash windows, with no destination or crash-window exclusions.
- Live-run golden fixtures for DuckDB, filesystem Parquet, and Postgres.
- Property/adversarial tests for the active contract verdict lattice, `SourcePosition` serialization, NDJSON parsing, and Singer/Airbyte parser inputs.
- The runtime conformance throughput rule remains active at `.10x/knowledge/runtime-conformance-throughput-rule.md`.

## Procedure

Closure review inspected the C1-C5 evidence and review records:

- `.10x/evidence/2026-07-08-p0-c1-run-spine-matrix-foundation.md`
- `.10x/reviews/2026-07-08-p0-c1-run-spine-matrix-foundation-review.md`
- `.10x/evidence/2026-07-08-p0-c2-rest-sql-run-matrix.md`
- `.10x/reviews/2026-07-08-p0-c2-rest-sql-run-matrix-review.md`
- `.10x/evidence/2026-07-08-p0-c3-cross-destination-chaos.md`
- `.10x/reviews/2026-07-08-p0-c3-cross-destination-chaos-review.md`
- `.10x/evidence/2026-07-08-p0-c4-live-run-goldens-per-destination.md`
- `.10x/reviews/2026-07-08-p0-c4-live-run-goldens-per-destination-review.md`
- `.10x/evidence/2026-07-08-p0-c5-property-fuzz-targets.md`
- `.10x/reviews/2026-07-08-p0-c5-property-fuzz-targets-review.md`

Final closure commands:

```text
cargo nextest run -p cdf-conformance --locked
rg -n "\.10x/tickets/2026-07-08-p0-c[1-5].*\.md|\.10x/tickets/2026-07-07-p0-workstream-c-spine-conformance-harness\.md" .10x
git diff --check
```

## Results

- `cargo nextest run -p cdf-conformance --locked`: pass; 60 tests run, 60 passed, 0 skipped, one expected slow live-run test, total 63.273s.
- `git diff --check`: pass.
- Reference scan before moving terminal records showed expected active Workstream C references; these were repaired after moving the parent and C6 tickets to `done/`.

## Acceptance mapping

- Matrix evidence lists executed and sheet-excluded cells for FILE, REST, and SQL sources across DuckDB, filesystem Parquet, and Postgres destinations and append, replace, and merge dispositions: C1 and C2.
- Chaos evidence exists for each MVP destination and ratified crash window: C3.
- Golden fixture evidence lists per-destination live-run hashes: C4.
- Property/fuzz target evidence lists exact commands, results, and tool limits: C5.
- C1-C5 each have evidence and adversarial review records.
- `.10x/knowledge/runtime-conformance-throughput-rule.md` is active and referenced by the closure review.

## What this supports

This supports closing `.10x/tickets/done/2026-07-07-p0-workstream-c-spine-conformance-harness.md` and `.10x/tickets/done/2026-07-08-p0-c6-workstream-c-closure.md`.

Because Workstreams A, B, and C are now closed, the P0 A-C stop-line for new destination lanes, new source-archetype lanes, and resident streaming-supervisor implementation lanes is lifted. The broader P0 program remains active until Workstreams E and F close.

## Limits

Native coverage-guided fuzz targets were intentionally not created in C5; bounded property/adversarial tests satisfy the active Workstream C requirement.

OSV remains nonzero only for the already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` advisory. Cargo audit with the scoped ignore, cargo deny, and cargo vet are passing in the relevant child evidence.

This evidence closes P0 Workstream C only. It does not close Workstream E contract-depth implementation, Workstream F benchmark gate, broader MVP acceptance demo scope, or the full CDF 1.0 parent.
