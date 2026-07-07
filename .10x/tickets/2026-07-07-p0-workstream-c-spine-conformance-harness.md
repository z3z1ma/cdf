Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-p0-structural-debt-program.md
Depends-On: .10x/tickets/done/2026-07-07-p0-workstream-a-streaming-commit-session.md, .10x/tickets/2026-07-07-p0-workstream-b-open-orchestrator-world.md

# P0 Workstream C: Make harnesses catch the run spine

## Scope

Bring `cdf-conformance`, golden fixtures, chaos coverage, and property/fuzz targets up to the general run spine so runtime changes are managed by tests rather than memory.

Owns:

- `crates/cdf-conformance/**`
- conformance fixtures and golden fixtures;
- property/fuzz target wiring where the workspace convention places them;
- targeted source/runtime test hooks only if required by conformance;
- `.10x/knowledge/runtime-conformance-throughput-rule.md` updates if implementation teaches a sharper rule.

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

## Blockers

Depends on Workstream A and B because the harness must target the final streaming session API and generic orchestration/chaos seam.
