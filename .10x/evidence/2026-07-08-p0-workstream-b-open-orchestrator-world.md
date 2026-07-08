Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-07-p0-workstream-b-open-orchestrator-world.md, .10x/decisions/project-destination-driver-registry.md

# P0 Workstream B Open Orchestrator World Evidence

## What Was Observed

Workstream B closed through four child tickets:

- B1 `.10x/tickets/done/2026-07-07-p0-b1-runtime-registry-foundation.md`
- B2 `.10x/tickets/done/2026-07-07-p0-b2-generic-package-replay-recovery.md`
- B3 `.10x/tickets/done/2026-07-07-p0-b3-generic-project-run-resolution.md`
- B4 `.10x/tickets/done/2026-07-07-p0-b4-caller-migration-wrapper-deletion.md`

The aggregate result:

- `crates/cdf-project/src/runtime.rs` is split into focused modules.
- `run_project` consumes trait-backed sources and registry-resolved destination runtimes.
- Package replay/recovery are generic over `ProjectDestinationRuntime`, kernel `DestinationProtocol`, segment-writing `CommitSession`, and trait-level receipt verification.
- CLI `run`, `replay package`, and `resume` delegate destination resolution/replay/recovery to project-owned generic paths.
- Conformance package replay/live-run helpers use generic request/report types.
- The temporary DuckDB/Parquet/Postgres replay/recover wrapper families and DuckDB-only failpoint wrappers are deleted.
- Failpoint/stage injection is destination-agnostic through generic `RuntimeStage` / package replay stage hooks.

The registered mock destination proof remains in `cdf-project` runtime tests and proves driver registration/resolution can drive generic run/replay/recovery/chaos seams without editing generic orchestrator logic.

## Procedure

Aggregate evidence records inspected:

- `.10x/evidence/2026-07-07-p0-b1-runtime-registry-foundation.md`
- `.10x/evidence/2026-07-08-p0-b2-generic-package-replay-recovery.md`
- `.10x/evidence/2026-07-08-p0-b3-generic-project-run-resolution.md`
- `.10x/evidence/2026-07-08-p0-b4-caller-migration-wrapper-deletion.md`

Aggregate review records inspected:

- `.10x/reviews/2026-07-07-p0-b1-runtime-registry-foundation-review.md`
- `.10x/reviews/2026-07-08-p0-b2-generic-package-replay-recovery-review.md`
- `.10x/reviews/2026-07-08-p0-b3-generic-project-run-resolution-review.md`
- `.10x/reviews/2026-07-08-p0-b4-caller-migration-wrapper-deletion-review.md`

Final B4 verification included:

- focused project/CLI/conformance check, clippy, test, nextest, doc, and cargo-hack gates;
- wrapper-deletion `rg` proof;
- Semgrep, CodeQL with reusable database, Gitleaks, OSV, cargo deny, cargo audit, cargo vet;
- jscpd, rust-code-analysis-cli, scc, and semver-checks.

## Results

Workstream B required outcomes are supported:

- Generic run orchestration composes trait-backed sources and destination runtimes.
- Generic package replay/recovery drives destination protocol/session/verification through one path.
- Adding a destination requires a driver/runtime registration rather than generic orchestrator/replay/recovery edits.
- Specialized public wrapper families were migrated and deleted rather than left as permanent compatibility paths.
- Failpoint injection applies to the generic replay stages.
- `runtime.rs` no longer owns orchestration, replay, recovery, failpoints, and reporting in one file.

Quality results from the closing B4 gate:

- `cargo nextest run -p cdf-project -p cdf-cli -p cdf-conformance --locked`: 208 passed.
- `cargo hack check` and `cargo hack clippy` over the three affected crates and each feature passed.
- Semgrep, Gitleaks, CodeQL, cargo deny, cargo vet, and focused unsafe scan found no new security issue.
- OSV and cargo audit report only the already-ratified `paste` advisory.
- `jscpd` over the broad B4 scope reported 6.819% duplicated lines.
- `rust-code-analysis-cli` reported `runtime/replay.rs` cyclomatic 114 and `replay_package_with_runtime` cyclomatic 19 as the remaining hotspots.

## What This Supports

This supports closing `.10x/tickets/done/2026-07-07-p0-workstream-b-open-orchestrator-world.md` and updating the P0 parent/coverage matrix to show Workstream B complete.

## Limits

Workstream B closure does not lift the P0 stop-line by itself. Workstream C remains open and still blocks new destination lanes, new source-archetype lanes, and the resident streaming supervisor.
