Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-p0-workstream-b-open-orchestrator-world.md
Depends-On: .10x/tickets/2026-07-07-p0-b1-runtime-registry-foundation.md, .10x/tickets/2026-07-07-p0-b2-generic-package-replay-recovery.md

# P0 B3: Generic project run and resolution

## Scope

Open `run_project` so the runtime consumes resource and destination traits after project resolution, instead of matching closed resource and destination enums inside orchestration.

Owns:

- generic project run orchestration over `&dyn ResourceStream` / `&dyn QueryableResource` and `ProjectDestinationRuntime`;
- `cdf-project` resource construction/resolution helpers for current file, REST, and SQL resources;
- `cdf-project` destination URI/project-config resolution through the built-in driver registry;
- CLI `run` migration to project-owned resolution;
- conformance live-run helper migration away from `run_local_file_to_duckdb_checkpoint`.

## Acceptance criteria

- `ProjectRunDestination` and `ProjectRunResource` are removed or reduced to non-public compatibility shims that are not used by generic orchestration.
- `run_project` or its replacement has no closed match over DuckDB/Parquet/Postgres destination variants.
- Local file, REST, and SQL resources enter orchestration as trait objects after dependency-backed construction.
- Non-file checkpointability validation is descriptor/capability driven rather than based on a closed `ProjectRunResource` enum.
- CLI `run` uses `cdf-project` destination/resource resolution rather than duplicating destination URI parsing.
- Existing `cdf-project`, `cdf-cli`, and `cdf-conformance` live-run tests pass through the generic run path.

## Evidence expectations

Record focused project/CLI/conformance tests, public API before/after inventory for run APIs, `rg` proof that closed run destination/resource enum matching is gone from orchestration, and complexity output for the run modules.

## Explicit exclusions

No new resource archetype, no new destination, no resident streaming supervisor, no full Workstream C matrix expansion, and no behavior changes to cursor/window semantics.

## Progress and notes

- 2026-07-07: Opened from Workstream B after caller inventory confirmed CLI run and conformance live-run are external users of the closed run API.

## Blockers

Depends on B1 and B2 because run finalization must reuse the generic package replay/commit gate rather than creating a second path.
