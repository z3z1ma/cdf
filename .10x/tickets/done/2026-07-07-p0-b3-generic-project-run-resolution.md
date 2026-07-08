Status: done
Created: 2026-07-07
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-07-p0-workstream-b-open-orchestrator-world.md
Depends-On: .10x/tickets/done/2026-07-07-p0-b1-runtime-registry-foundation.md, .10x/tickets/done/2026-07-07-p0-b2-generic-package-replay-recovery.md

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
- 2026-07-08: Unblocked by B2 closure. Run finalization must reuse `.10x/tickets/done/2026-07-07-p0-b2-generic-package-replay-recovery.md` rather than adding a second destination commit/checkpoint path.
- 2026-07-08: Activated after B2 closure. Assigned to worker subagent for generic run orchestration, project-owned resource/destination resolution, CLI `run` migration, and conformance live-run migration.
- 2026-07-08: Replacement worker audited the inherited partial B3 diff, kept the trait-backed run resource/destination path, repaired rustfmt drift, and verified the generic run path through `cdf-project` runtime tests, `cdf-cli` run tests, `cdf-conformance` live-run tests, workspace `cargo check`, and `clippy`.
- 2026-07-08: Parent tightened and closed the worker diff: removed the old public closed enum names from Rust source, routed CLI destination resolution through the project registry, preserved lazy Parquet validation-before-write behavior, split destination adapters into focused submodules, ran final quality/security checks, and recorded evidence in `.10x/evidence/2026-07-08-p0-b3-generic-project-run-resolution.md`.
- 2026-07-08: Adversarial review recorded at `.10x/reviews/2026-07-08-p0-b3-generic-project-run-resolution-review.md`; residual wrapper-family deletion remains owned by B4.

## Blockers

None. B1 and B2 are closed.
