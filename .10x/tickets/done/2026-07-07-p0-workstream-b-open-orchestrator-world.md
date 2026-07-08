Status: done
Created: 2026-07-07
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-07-p0-structural-debt-program.md
Depends-On: .10x/tickets/done/2026-07-07-p0-workstream-a-streaming-commit-session.md

# P0 Workstream B: Open the orchestrator world

## Scope

Refactor `cdf-project` so orchestration, replay, recovery, and chaos paths are generic over kernel traits and driver registration rather than closed resource/destination enums and destination-specialized wrapper families.

This ticket is a parent plan. Child tickets own executable implementation slices.

Owns:

- `crates/cdf-project/src/runtime.rs` split into focused modules.
- `cdf-project` registry/factory/resolution code for project config and URI schemes.
- Caller migration in CLI, conformance, golden, and chaos tests.
- Deletion of temporary specialized replay/recover wrappers after all callers migrate.

## Child tickets

- `.10x/tickets/done/2026-07-07-p0-b1-runtime-registry-foundation.md`
- `.10x/tickets/done/2026-07-07-p0-b2-generic-package-replay-recovery.md`
- `.10x/tickets/done/2026-07-07-p0-b3-generic-project-run-resolution.md`
- `.10x/tickets/done/2026-07-07-p0-b4-caller-migration-wrapper-deletion.md`

## Required outcome

- The orchestrator composes `ResourceStream`/`QueryableResource` with `DestinationProtocol`/`CommitSession` through generic runtime paths.
- `cdf-project` owns resolution from project config or URI scheme to concrete drivers, not destination-specific orchestration logic.
- Adding a destination requires a driver crate and registration, with zero edits to generic orchestrator, replay, recovery, or chaos logic.
- Specialized function families such as `replay_duckdb_package_from_artifacts`, `recover_parquet_package_from_artifacts`, `replay_postgres_package_from_artifacts`, and DuckDB-only failpoint wrappers are migrated and deleted rather than preserved as permanent compatibility paths.
- Failpoint injection becomes destination-agnostic on the generic path.
- `runtime.rs` is split so no one module owns orchestration, replay, recovery, failpoints, and reporting simultaneously.

## Acceptance criteria

- A before/after public API inventory for `cdf-project` records removed specialized wrappers, retained compatibility surface if any, and all caller migrations.
- A mock destination registration test proves plan -> run -> replay -> recover -> chaos can use a registered destination without editing orchestrator logic.
- CLI, conformance, golden, and chaos callers route through the generic path.
- `runtime.rs` is split according to `.10x/knowledge/rust-crate-organization.md`, and no new monolithic module replaces it.
- `rust-code-analysis-cli` shows the `cdf-project` runtime hotspot reduced or justified, and `jscpd` does not introduce unaccepted duplication.

## Evidence expectations

Record focused `cdf-project` tests, downstream CLI/conformance tests, wrapper-deletion proof by `rg`, public API inventory, complexity/duplication metrics, and adversarial review.

## Explicit exclusions

No new destination implementation, no new source archetype, no distributed scheduler, no CDC/Kafka lane, and no behavior changes outside the genericization necessary to preserve existing run/replay/recover semantics.

## Progress and notes

- 2026-07-07: Opened from P0 stop-line. Current inspection shows closed enums `ProjectRunDestination` and `ProjectRunResource` plus a 2,913-line `crates/cdf-project/src/runtime.rs` with public specialized replay/recover families.
- 2026-07-07: Read-only subagent inventory confirmed `crates/cdf-project/src/runtime_tests.rs` is also a 3,290-line hotspot, and that prior run-spine records intentionally preserved compatibility wrappers rather than owning their deletion.
- 2026-07-07: Read-only subagents inventoried public APIs/callers, module split candidates, and the smallest generic adapter shape. API decision recorded in `.10x/decisions/project-destination-driver-registry.md`.
- 2026-07-07: Split Workstream B into four executable children: B1 runtime registry/module foundation, B2 generic package replay/recovery, B3 generic project run/resolution, and B4 caller migration/wrapper deletion/closure.
- 2026-07-07: Closed B1 at `.10x/tickets/done/2026-07-07-p0-b1-runtime-registry-foundation.md`. `cdf-project` runtime is now split into focused modules, the project destination driver/runtime foundation API exists, and existing public compatibility APIs remain for B2-B4 migration.
- 2026-07-08: Closed B2 at `.10x/tickets/done/2026-07-07-p0-b2-generic-package-replay-recovery.md`. Package replay/recovery now delegate through the generic `ProjectDestinationRuntime` skeleton, trait-level destination verification, and segment-writing sessions; a mock registered destination test proves generic replay/recovery/failpoint injection without orchestrator edits.
- 2026-07-08: Closed B3 at `.10x/tickets/done/2026-07-07-p0-b3-generic-project-run-resolution.md`. Project runs now consume trait-backed resources and registry-resolved destination runtimes; CLI `run` delegates destination resolution to `cdf-project`; old public closed run resource/destination enum names are gone from Rust source.
- 2026-07-08: Closed B4 at `.10x/tickets/done/2026-07-07-p0-b4-caller-migration-wrapper-deletion.md`. CLI replay/resume and conformance package-replay/live-run callers now use generic replay/recovery APIs; the public DuckDB/Parquet/Postgres wrapper family and DuckDB-only failpoint wrappers are deleted.
- 2026-07-08: Workstream B aggregate evidence recorded in `.10x/evidence/2026-07-08-p0-workstream-b-open-orchestrator-world.md`; adversarial review recorded in `.10x/reviews/2026-07-08-p0-workstream-b-open-orchestrator-world-review.md`. Workstream B is done. The P0 stop-line remains active because Workstream C is still open.

## Blockers

None.
