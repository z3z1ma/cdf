Status: done
Created: 2026-07-07
Updated: 2026-07-07

# Open Orchestrator World Inventory

## Question

What exact source shape, public API surface, caller set, and adapter decision are required to execute P0 Workstream B without turning `cdf-project` into another monolithic vertical slice?

## Sources and methods

Read-only inspection covered:

- `.10x/tickets/2026-07-07-p0-workstream-b-open-orchestrator-world.md`
- `.10x/decisions/commit-session-segment-write-api.md`
- `.10x/specs/run-orchestration-ledger.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/knowledge/rust-crate-organization.md`
- `crates/cdf-project/src/lib.rs`
- `crates/cdf-project/src/runtime.rs`
- `crates/cdf-project/src/runtime_tests.rs`
- `crates/cdf-cli/src/run_command.rs`
- `crates/cdf-cli/src/replay_command.rs`
- `crates/cdf-cli/src/resume_command/attempt.rs`
- `crates/cdf-cli/src/resume_command/destination.rs`
- `crates/cdf-conformance/src/live_run/mod.rs`
- `crates/cdf-conformance/src/package_replay/mod.rs`
- `crates/cdf-conformance/src/package_replay/tests.rs`
- first-party destination session/planning files in `cdf-dest-duckdb`, `cdf-dest-parquet`, and `cdf-dest-postgres`

Subagents Huygens, Newton, and Euler independently inventoried callers, module split candidates, and the smallest registry/adapter shape. The parent reconciled their results against source.

## Findings

`cdf-project` exports runtime symbols via `pub use runtime::*` in `crates/cdf-project/src/lib.rs`.

Current public runtime surface includes closed enums:

- `ProjectRunDestination::{DuckDb, ParquetFilesystem, Postgres}`;
- `ProjectRunResource::{LocalFile, Rest, Sql}`;
- DuckDB-only `LocalDuckDbLifecycleFailpoint` and `LocalDuckDbLifecycleFailpointHook`.

Current public runtime surface also includes destination-specialized run/replay/recover APIs:

- DuckDB local run wrappers;
- DuckDB artifact replay/recover wrappers and failpoint variants;
- DuckDB prepared replay/recover wrappers and failpoint variants;
- Parquet artifact replay/recover wrappers;
- Postgres artifact replay/recover wrappers;
- destination-specific request/report types for all of the above.

External callers that must migrate before wrapper deletion:

- CLI `run`: constructs `ProjectRunResource` and `ProjectRunDestination` in `crates/cdf-cli/src/run_command.rs`.
- CLI `replay package`: has a closed `ReplayDestination` enum and calls DuckDB/Parquet/Postgres artifact replay wrappers in `crates/cdf-cli/src/replay_command.rs`.
- CLI `resume`: has a closed `SelectedDestination` enum and calls DuckDB/Parquet/Postgres replay/recover wrappers in `crates/cdf-cli/src/resume_command/attempt.rs`.
- CLI tests directly import DuckDB artifact replay for fixture validation.
- `cdf-conformance` live-run helpers call the DuckDB-only local run wrapper.
- `cdf-conformance` package replay/chaos helpers import DuckDB-only prepared/artifact replay and failpoint wrappers.
- `cdf-project` runtime tests import and call the specialized APIs directly.

`crates/cdf-project/src/runtime.rs` currently owns multiple unrelated responsibilities in one file: public DTOs, resource enum adapters, run orchestration, validation, state-delta/cursor math, run ledger event recording, destination replay/recovery, session segment feeding, failpoints, receipt verification, and destination-specific planning helpers.

Destination constraints:

- DuckDB and Parquet require package-aware planning and currently store pending session context keyed by `PlanId`.
- Postgres requires a `PostgresLoadPlan` before `DestinationProtocol::begin` can safely execute. That plan depends on package schema columns, target parsing, dedup policy, existing-table context, merge keys, state delta, and destination DDL/mirror/verification clauses.
- Kernel `DestinationProtocol::plan_commit` is not enough to replace this package-aware planning, and the kernel must remain package-free.

Resource constraints:

- The generic runtime can consume `ResourceStream`/`QueryableResource`, but construction is project-specific.
- `CompiledResource` directly executes only file resources; REST and SQL need dependency-backed runtime resources.
- Non-file checkpointability currently depends on ordered cursor semantics and should become descriptor/capability driven rather than enum-variant driven.

## Conclusions

The smallest safe Workstream B shape is a `cdf-project` destination driver registry plus object-safe project destination runtime adapters. The decision is recorded in `.10x/decisions/project-destination-driver-registry.md`.

The implementation should split into four child tickets:

- `.10x/tickets/2026-07-07-p0-b1-runtime-registry-foundation.md`
- `.10x/tickets/2026-07-07-p0-b2-generic-package-replay-recovery.md`
- `.10x/tickets/2026-07-07-p0-b3-generic-project-run-resolution.md`
- `.10x/tickets/2026-07-07-p0-b4-caller-migration-wrapper-deletion.md`

The kernel should not be changed for package-aware planning in Workstream B. Generic project runtime must verify the package before segment feeding, ask the adapter for package-aware commit preparation, then drive kernel `DestinationProtocol::begin`, `CommitSession::write_segment`, `finalize`, and `DestinationProtocol::verify`.

## Limits

This is a source/record inventory, not implementation evidence. It does not prove the generic runtime exists or that wrappers are deleted.

Line numbers in source will drift during B implementation. The durable conclusions are the caller sets, module boundaries, and project-level adapter decision.
