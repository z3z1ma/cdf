Status: active
Created: 2026-07-07
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-07-p0-structural-debt-program.md
Depends-On: .10x/tickets/done/2026-07-07-p0-workstream-a-streaming-commit-session.md, .10x/tickets/done/2026-07-07-p0-workstream-b-open-orchestrator-world.md

# P0 Workstream C: Make harnesses catch the run spine

## Scope

Bring `cdf-conformance`, golden fixtures, chaos coverage, and property/fuzz targets up to the general run spine so runtime changes are managed by tests rather than memory.

Owns:

- `crates/cdf-conformance/**`
- conformance fixtures and golden fixtures;
- property/fuzz target wiring where the workspace convention places them;
- targeted source/runtime test hooks only if required by conformance;
- `.10x/knowledge/runtime-conformance-throughput-rule.md` updates if implementation teaches a sharper rule.

This ticket is a parent plan. Child tickets own executable implementation slices.

## Child Tickets

- `.10x/tickets/done/2026-07-08-p0-c1-run-spine-matrix-foundation.md`
- `.10x/tickets/done/2026-07-08-p0-c2-rest-sql-run-matrix.md`
- `.10x/tickets/done/2026-07-08-p0-c3-cross-destination-chaos.md`
- `.10x/tickets/done/2026-07-08-p0-c4-live-run-goldens-per-destination.md`
- `.10x/tickets/2026-07-08-p0-c5-property-fuzz-targets.md`
- `.10x/tickets/2026-07-08-p0-c6-workstream-c-closure.md`

## Required outcome

- Conformance consumes `run_project` or its generic successor directly.
- A scenario matrix covers source archetypes `file`, deterministic REST fixture, and SQL against destinations DuckDB, Parquet, and Postgres across append, replace, and merge where sheets support them.
- Each cell asserts plan honesty, package validity, receipt verification through the trait, checkpoint gating, replay identity, and duplicate handling.
- Sheet-excluded cells are recorded as excluded with reason, not silently skipped.
- Chaos failpoints run through the Workstream B generic seam for all ratified crash windows against DuckDB, Parquet, and Postgres.
- Golden fixtures exist per destination for at least one live-run cell each.
- Property/fuzz targets cover contract verdict-lattice totality, position serialization round-trips across `state_version`, and NDJSON/Singer/Airbyte parser adversarial input.
- The runtime conformance throughput rule is active and referenced by closure reviews.

## Acceptance criteria

- Matrix output is committed as an evidence record with executed cells and excluded cells.
- Chaos evidence exists per MVP destination.
- New golden hashes are committed and explained.
- Property/fuzz targets are wired into the quality cadence with exact commands and runtime expectations.
- Parent/runtime closure reviews can cite conformance ownership for new or changed runtime paths.

## Evidence expectations

Record matrix run output, chaos output, golden fixture hashes, property/fuzz target commands, mutation evidence for harness logic where feasible, jscpd/rust-code-analysis metrics for conformance additions, and adversarial review.

## Explicit exclusions

No new product runtime behavior except narrow hooks required for conformance. No new destination/source lane beyond the existing MVP file/REST/SQL and DuckDB/Parquet/Postgres surfaces.

## Progress and notes

- 2026-07-07: Opened from P0 stop-line. Existing conformance covers important slices, but the full run spine matrix, non-DuckDB chaos, per-destination live-run golden fixtures, and property/fuzz targets are still open.
- 2026-07-07: Read-only subagent inventory found no `proptest`, `quickcheck`, or fuzz dependency in `crates/cdf-conformance/Cargo.toml`; property/fuzz target selection remains this workstream's implementation responsibility.
- 2026-07-08: Workstream B dependency closed. This ticket is now the remaining A-C stop-line blocker.
- 2026-07-08: Split Workstream C into executable children: C1 matrix foundation/file cells, C2 REST/SQL matrix cells, C3 cross-destination chaos, C4 per-destination live-run goldens, C5 property/fuzz targets, and C6 closure rollup.
- 2026-07-08: C1 closed. Conformance now has a FILE-source run-spine matrix covering DuckDB, filesystem Parquet, and Postgres across supported append/replace/merge cells, with Parquet merge recorded as a sheet-backed exclusion.
- 2026-07-08: C2 closed. The run-spine matrix now covers FILE, deterministic REST fixture, and table-backed Postgres SQL source archetypes across DuckDB, filesystem Parquet, and Postgres destinations, with 24 executed cells and 3 sheet-backed Parquet merge exclusions.
- 2026-07-08: C3 closed. Generic runtime chaos now covers DuckDB, filesystem Parquet, and Postgres across all four ratified crash windows through `RuntimeStage`, with no destination exclusions and evidence/review recorded.
- 2026-07-08: C4 closed. Live-run golden fixtures now exist for DuckDB, filesystem Parquet, and Postgres with committed hashes, package verification before comparison, destination row-count evidence, trait-level receipt verification, repeated determinism runs, and evidence/review recorded.

## Blockers

None. Workstreams A and B are closed; the harness can target the final streaming session API and generic orchestration/chaos seam.
