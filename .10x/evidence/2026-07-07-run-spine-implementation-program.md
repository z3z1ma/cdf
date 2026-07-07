Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-run-spine-implementation-program.md, .10x/decisions/run-ledger-commit-session-spine.md, .10x/specs/run-orchestration-ledger.md

# Run spine implementation program evidence

## What was observed

All children of the P0 run-spine implementation parent are closed:

- Kernel destination commit-session API: `.10x/tickets/done/2026-07-07-kernel-destination-commit-session-api.md`
- DuckDB commit-session refactor: `.10x/tickets/done/2026-07-07-duckdb-commit-session-refactor.md`
- Parquet commit-session refactor: `.10x/tickets/done/2026-07-07-parquet-commit-session-refactor.md`
- Postgres commit-session refactor: `.10x/tickets/done/2026-07-07-postgres-commit-session-refactor.md`
- SQLite run-ledger store: `.10x/tickets/done/2026-07-07-run-ledger-store.md`
- General run orchestrator: `.10x/tickets/done/2026-07-07-general-run-orchestrator.md`
- Non-file window-close checkpoint semantics: `.10x/tickets/done/2026-07-07-non-file-window-close-checkpoint-semantics.md`
- CLI run/resume/replay/inspect spine: `.10x/tickets/done/2026-07-07-cli-run-resume-replay-inspect-spine.md`

Together these records show the ratified run model, destination `CommitSession` abstraction, destination implementations, run ledger persistence, general project run orchestration, checkpoint advancement semantics, and CLI command consumers.

## Procedure

Parent integration commands run from `/Users/alexanderbut/code_projects/personal/firn`:

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo test -p cdf-cli run_sql_resource --locked -- --nocapture`: passed.
- `cargo test -p cdf-project general_project_run_executes_table_backed_postgres_sql_resource_stream --locked -- --nocapture`: passed.
- `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo test --workspace --locked --no-fail-fast`: passed across workspace unit tests and doc tests.
- `cargo deny check`: passed, with existing duplicate-version warnings only.
- `cargo audit`: passed with the already-ratified `paste` advisory warning.
- `cargo machete --with-metadata`: passed.
- Semgrep and Gitleaks passed on the touched CLI test file for the final child slice.
- `rust-code-analysis-cli`, Jscpd, and SCC passed over `crates/cdf-cli/src` for the final child slice.

CodeQL was intentionally not rerun for this final test-only slice under the active standing-goal quality note to skip CodeQL unless the change is complex/high-risk or part of a dedicated deep batch. Earlier run-spine children recorded their own focused quality evidence.

## What this supports

This supports the run-spine parent acceptance criteria:

- Kernel, destinations, project runtime, run ledger, and CLI compose through the ratified run spine.
- Destination-specific commit behavior is expressed through `CommitSession` implementations.
- Existing specialized DuckDB/file runtime functions remain as compatibility wrappers while the general orchestrator is the shared spine.
- Checkpoint head advancement remains receipt-gated through `CheckpointStore::commit`; the non-file cursor slice adds fail-closed window-close semantics for ratified cursor types.
- The MVP demonstration path can now consume the run spine rather than adding another source/destination-specialized runtime path.

## Limits

This parent closure does not close the full CDF system, the full CLI surface, or the MVP killer-demo harness. Remaining owners include `.10x/tickets/2026-07-05-cli-surface.md`, `.10x/tickets/2026-07-07-cli-remaining-command-planners.md`, and `.10x/tickets/2026-07-05-conformance-chaos-golden.md`.
