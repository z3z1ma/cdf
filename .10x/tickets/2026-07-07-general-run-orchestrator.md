Status: blocked
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-run-spine-implementation-program.md
Depends-On: .10x/tickets/done/2026-07-07-duckdb-commit-session-refactor.md, .10x/tickets/done/2026-07-07-parquet-commit-session-refactor.md, .10x/tickets/done/2026-07-07-postgres-commit-session-refactor.md, .10x/tickets/done/2026-07-07-run-ledger-store.md, .10x/specs/run-orchestration-ledger.md

# Implement the general project run orchestrator

## Scope

Replace the hard-coded local-file-to-DuckDB orchestration path with a general project runtime over `ResourceStream`, package building, destination commit sessions, checkpoint stores, and the run ledger.

Owns:

- `crates/cdf-project/src/runtime.rs` and supporting modules.
- Compatibility wrappers for current local DuckDB/file conformance callers.
- Focused integration tests for file, REST, and table-backed Postgres SQL resources where lower-layer runtime dependencies are deterministic.

## Acceptance criteria

- One orchestrator can run supported resource streams into supported destinations through the commit-session API.
- The orchestrator records run ledger events in the order specified by `.10x/specs/run-orchestration-ledger.md`.
- Existing local-file-to-DuckDB tests and golden conformance pass through compatibility wrappers with no unnecessary fixture churn.
- Crash-window recovery uses package/receipt/checkpoint evidence and does not contact sources after package finalization.
- Unsupported combinations fail closed before source, package, destination, or checkpoint mutation.

## Evidence expectations

Run focused `cdf-project` tests, conformance live-run/chaos tests, relevant destination/source tests, clippy, nextest over touched crates, and review mapping each crash window to evidence.

## Explicit exclusions

No CLI command parsing, no `inspect run` presentation, no distributed scheduler, no resident streaming, no arbitrary SQL query execution, no live external HTTP credentials, no warehouse destinations, no performance optimization.

## Design notes

- 2026-07-07: Destination commit-session refactors proved the current portable kernel `DestinationProtocol::begin(request, plan)` inputs are not enough by themselves to commit a package. DuckDB and Parquet require package path/schema/package replay context captured by package-aware planning, and Postgres currently uses `with_commit_request` as a compatibility handoff. This ticket must either route generic runs through package-aware destination planning before `begin`, or explicitly supersede/refine the kernel session input contract so package replay inputs are first-class.
- 2026-07-07: The run-ledger store exposes per-run event and snapshot queries plus indexed persisted pointers. This orchestrator should use those APIs for run-scoped recovery/inspection and define any broader interrupted-run discovery policy needed by CLI `resume`, rather than pushing that policy into the storage layer.

## Blockers

- Postgres destination support is split to `.10x/tickets/2026-07-07-general-run-postgres-destination.md` because the current project-run input does not ratify how to construct `PostgresLoadPlanInput` values such as `PostgresTarget`, column mappings, merge keys, dedup policy, and existing-table policy.

## Progress and notes

- 2026-07-07: Implemented the first general project-run facade in `crates/cdf-project/src/runtime.rs` for deterministic local file resources into DuckDB. Existing local-file-to-DuckDB compatibility functions now delegate through `ProjectRunRequest`/`run_project` while preserving their public report shape. The run ledger uses the existing `SqliteRunLedger` API from `cdf-state-sqlite`; no storage API change was needed.
- 2026-07-07: The project runtime records `run_started`, `plan_recorded`, `package_started`, `package_finalized`, `checkpoint_proposed`, `destination_commit_started`, `destination_receipt_recorded`, `checkpoint_committed`, `package_status_updated`, `run_succeeded`, and `run_failed` around the existing package/checkpoint flow. The ledger remains observational only; checkpoint advancement is still only through `CheckpointStore::commit(checkpoint_id, receipt)`.
- 2026-07-07: DuckDB package replay now plans package-aware commits, then begins and drives the kernel `DestinationProtocol::begin` commit session. Compatibility duplicate/package-receipt report fields are preserved by inspecting public DuckDB mirror/package receipt evidence around the session commit.
- 2026-07-07: Added focused `cdf-project` tests for run-ledger event ordering, unsupported-source fail-closed behavior before package/destination/state DB creation, post-receipt failure recording without checkpoint advancement, and package/receipt recovery after a general run failure without a source handle.
- 2026-07-07: Verification passed: `cargo fmt --all -- --check`; `cargo test -p cdf-project --locked --no-fail-fast`; `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`; `cargo test -p cdf-conformance --locked --no-fail-fast`; targeted `cargo test -p cdf-cli --locked run_local_file_to_duckdb_commits_package_rows_mirrors_and_checkpoint`; `cargo check --workspace --all-targets --locked`; `git diff --check`.
- 2026-07-07: Continued after parent review. Added filesystem Parquet as a second deterministic `ProjectRunDestination`, using package replay inputs, `ParquetDestination::plan_package_commit`, and `DestinationProtocol::begin`. Added Parquet run tests for commit-session ledger ordering, unsupported `merge` fail-closed behavior before state/package/destination mutation, and post-receipt artifact recovery without source contact. `Cargo.lock` was refreshed for the new direct `cdf-project -> cdf-dest-parquet` workspace dependency.
- 2026-07-07: Split remaining unratified scope into `.10x/tickets/2026-07-07-general-run-postgres-destination.md` and `.10x/tickets/done/2026-07-07-general-run-non-file-resource-streams.md`; this parent remains blocked and not closable until those blockers are resolved or the parent scope is superseded.
- 2026-07-07: Verification after Parquet continuation passed: `cargo fmt --all -- --check`; `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`; `cargo test -p cdf-project --locked --no-fail-fast` with 42 unit tests and 0 doc tests; `cargo nextest run -p cdf-project --locked` with 42 passed; `cargo test -p cdf-dest-parquet --locked --no-fail-fast` with 18 unit tests and 0 doc tests; `cargo test -p cdf-conformance --locked --no-fail-fast` with 40 unit tests and 0 doc tests; targeted `cargo test -p cdf-cli --locked run_local_file_to_duckdb_commits_package_rows_mirrors_and_checkpoint` with 1 matched test passing; `cargo check --workspace --all-targets --locked`; `git diff --check`.
- 2026-07-07: Parent verification recorded in `.10x/evidence/2026-07-07-general-run-orchestrator-partial.md` and adversarial review recorded in `.10x/reviews/2026-07-07-general-run-orchestrator-partial-review.md`. The implemented slice is usable for local file resources into DuckDB and filesystem Parquet, but this ticket is not done.
- 2026-07-07: Closed non-file source-stream blocker via `.10x/tickets/done/2026-07-07-general-run-non-file-resource-streams.md`: the general orchestrator now accepts dependency-bearing `RestResource` and `SqlResource` wrappers and supports exact zero-lag cursor checkpointing for deterministic REST and table-backed Postgres SQL sources. Broader inexact/lagged cursor and page-token checkpoint semantics are tracked separately in `.10x/tickets/2026-07-07-non-file-window-close-checkpoint-semantics.md` and no longer block this parent slice.
