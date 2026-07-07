Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/2026-07-07-cli-replay-package-spine.md
Verdict: concerns

# CLI DuckDB Package Replay Review

## Target

Partial implementation for `.10x/tickets/2026-07-07-cli-replay-package-spine.md`, covering `cdf replay package <DIR> --to duckdb://path` only.

## Findings

- Significant: The ticket acceptance criteria are not fully satisfied. Filesystem Parquet replay is still blocked on ratified CLI destination URI spelling, and Postgres replay is still blocked on explicit target, merge-dedup, and existing-table policy CLI input shape. The ticket correctly remains `blocked`.
- No finding: DuckDB package replay now uses package artifacts through `cdf_project::replay_duckdb_package_from_artifacts`, creates a run id, commits checkpoint state, appends a `replay_recorded` ledger event, and reports the required package, destination, receipt, checkpoint, duplicate/no-op, and package-status fields.
- No finding: The CLI rejects missing `--to`, unsupported destination schemes, unratified Parquet syntax, and Postgres policy gaps before replay mutation. Parent review also added coverage that missing package artifacts fail before creating a DuckDB destination parent or checkpoint state.
- No finding: Source-loss replay is covered at the CLI layer by deleting the original source file and state store before replay; the successful replay proves this slice does not re-contact the source resource.

## Verdict

Concerns raised. The DuckDB slice is acceptable progress to commit, but the ticket must not close until the Parquet and Postgres blockers are ratified and implemented or explicitly descoped.

## Residual risk

CodeQL was not rerun for this focused slice to avoid refreshing the reusable database for a small CLI-only change. The parent evidence records focused Rust, supply-chain, Semgrep, and unsafe-scan coverage, but not current-tree CodeQL coverage.
