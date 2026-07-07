Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-general-run-orchestrator.md, .10x/tickets/done/2026-07-07-general-run-postgres-destination.md, .10x/tickets/done/2026-07-07-general-run-non-file-resource-streams.md, .10x/tickets/done/2026-07-07-non-duckdb-package-replay-recovery.md

# General run orchestrator closure evidence

## What was observed

The general project runtime now has one public `run_project(ProjectRunRequest)` facade over `ProjectRunResource` and `ProjectRunDestination`. It supports local file resources, exact zero-lag REST resources, and table-backed Postgres SQL resources in the accepted slice, and it commits to DuckDB, filesystem Parquet, and Postgres destinations through destination commit sessions after package finalization.

Legacy local-file-to-DuckDB callers still delegate through the general project runtime and preserve their compatibility report shape. Checkpoint advancement remains receipt-gated through `CheckpointStore::commit`; the run ledger records ordered observational events and is not a state authority.

Parent closure was blocked by missing finalized-package/no-durable-receipt replay for Parquet/Postgres. That gap is now closed by `.10x/tickets/done/2026-07-07-non-duckdb-package-replay-recovery.md`.

## Acceptance mapping

- One orchestrator can run supported resource streams into supported destinations through commit-session API: covered by `cdf-project` tests for file to DuckDB/Parquet/Postgres, deterministic REST to DuckDB, and table-backed Postgres SQL to DuckDB; destination session tests for DuckDB, Parquet, and Postgres remain green.
- Ledger event order follows `.10x/specs/run-orchestration-ledger.md`: covered by `general_project_run_records_ledger_events_in_commit_gate_order`, Parquet and Postgres ledger-order tests, and failure ledger tests.
- Existing local-file-to-DuckDB tests and golden conformance pass through compatibility wrappers: covered by `cdf-project`, `cdf-cli` prior focused evidence, and `cdf-conformance` golden/live-run tests.
- Crash-window recovery uses package/receipt/checkpoint evidence without source contact after package finalization: covered by DuckDB conformance crash-window tests, durable-receipt recovery tests for DuckDB/Parquet/Postgres, and new no-receipt package-artifact replay tests for Parquet/Postgres.
- Unsupported combinations fail closed before source, package, destination, or checkpoint mutation: covered by raw REST/SQL compiled-resource rejection, REST/SQL dependency/cursor rejection, Parquet unsupported merge rejection, Postgres unsupported schema rejection, and Postgres replay target mismatch rejection.

## Procedure

Closure uses the accumulated child evidence plus current-tree verification from `.10x/evidence/2026-07-07-non-duckdb-package-replay-recovery.md`.

Current-tree checks observed for this closure:

- `cargo fmt --all -- --check`: passed.
- `git diff --check -- . ':(exclude).gitignore'`: passed.
- `cargo test -p cdf-project --locked artifact_replay -- --nocapture`: passed, 6 tests.
- `cargo test -p cdf-project --locked --no-fail-fast`: passed, 56 unit tests and 0 doc tests.
- `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`: passed.
- `cargo test -p cdf-dest-parquet --locked --no-fail-fast`: passed, 18 unit tests and 0 doc tests.
- `cargo test -p cdf-dest-postgres --locked --no-fail-fast`: passed, 27 unit/live tests and 0 doc tests.
- `cargo test -p cdf-conformance --locked --no-fail-fast`: passed, 40 unit tests and 0 doc tests.
- `cargo check --workspace --all-targets --locked`: passed.
- Security/supply-chain gates for the closing diff are recorded in `.10x/evidence/2026-07-07-non-duckdb-package-replay-recovery.md`: `cargo deny`, `cargo vet`, `cargo audit`, OSV, Semgrep, and CodeQL analyze-only against the reusable DB.

## Limits

CLI `run`, `resume`, `replay package`, and `inspect run` are not closed by this ticket. They are owned by `.10x/tickets/2026-07-07-cli-run-resume-replay-inspect-spine.md`, now unblocked by the general orchestrator closure.
