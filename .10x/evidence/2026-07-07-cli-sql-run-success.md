Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-cli-sql-run-success.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md, .10x/tickets/done/2026-07-07-declarative-postgres-sql-resource-execution.md, .10x/tickets/done/2026-07-07-general-run-non-file-resource-streams.md

# CLI SQL run success evidence

## What was observed

`crates/cdf-cli/src/run_command.rs` already constructs SQL CLI run resources with `SqlRuntimeDependencies::new().with_secret_provider(context.secret_provider())`, so no CLI runtime wiring change was required.

`crates/cdf-cli/src/tests.rs` now includes `run_sql_resource_with_ordered_cursor_commits_checkpoint`, a live local Postgres CLI test that:

- creates a live Postgres source table with `id` and `updated_at`;
- stores the resolved Postgres source DSN in a `secret://file/sql-dsn` reference;
- declares a table-backed `warehouse.orders` SQL resource with `cursor = { field = "updated_at", ordering = "exact", lag = "0ms" }`;
- executes `cdf run` through `run_valid_run_resource_target`;
- asserts stable JSON fields for resource id, target, DuckDB destination, row count, receipt row count, committed checkpoint, and terminal `run_succeeded` ledger event;
- asserts the resolved source DSN and inserted marker secret are absent from CLI stdout/stderr;
- reads the DuckDB destination table and verifies rows `(1, 10)` and `(2, 20)`;
- reads the SQLite checkpoint head and verifies the output position is `SourcePosition::Cursor(updated_at = I64(20))`.

The existing SQL fail-closed CLI tests for missing secret resolution and missing ordered cursor remain in the same `run_sql_resource` test filter.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

- `cargo fmt --all -- --check`: passed after one `cargo fmt --all` formatting pass.
- `cargo test -p cdf-cli run_sql_resource --locked -- --nocapture`: passed, 3 matched CLI SQL tests including the new live Postgres success path.
- `cargo test -p cdf-project general_project_run_executes_table_backed_postgres_sql_resource_stream --locked -- --nocapture`: passed, 1 matched live project-runtime SQL test.
- `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo test --workspace --locked --no-fail-fast`: passed across workspace unit tests and doc tests.
- `cargo deny check`: passed, with only the existing duplicate-version warnings.
- `cargo audit`: passed with the already-ratified `paste` advisory warning.
- `cargo machete --with-metadata`: passed; no unused dependencies reported.
- `git diff --check`: passed.
- `rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|\bSend\b|\bSync\b" crates/cdf-cli/src/tests.rs`: no matches; command exited 1 because there were no matches.
- `semgrep scan --no-git-ignore --config p/rust --config p/security-audit --json --output reports/ai-quality/semgrep-cli-sql-run-success.json crates/cdf-cli/src/tests.rs`: passed; 0 findings.
- `gitleaks dir --no-banner --redact --report-format json --report-path reports/ai-quality/gitleaks-cli-sql-run-success.json crates/cdf-cli/src/tests.rs`: passed; no leaks found.
- `rust-code-analysis-cli -m -O json -p crates/cdf-cli/src > reports/ai-quality/rust-code-analysis-cli-sql-run-after-cli.json`: passed.
- `jscpd crates/cdf-cli/src --reporters json --output reports/ai-quality/jscpd-cli-sql-run-after-cli`: passed.
- `scc --format json --output reports/ai-quality/scc-cli-sql-run-after-cli.json crates/cdf-cli/src`: passed.

Quality metrics recorded from before/after CLI reports:

- Jscpd over `crates/cdf-cli/src`: 32 sources in both runs; duplicated lines moved from 890 / 12017 lines / 7.4061745860031625% to 907 / 12141 lines / 7.470554320072481%. The increase is from the focused integration fixture added to `tests.rs`.
- Jscpd duplicated tokens moved from 8.671975408424204% to 8.721912872584344%.
- `crates/cdf-cli/src/tests.rs` rust-code-analysis metrics moved from cognitive 38 / cyclomatic 251 / SLOC 4855 / functions 180 to cognitive 40 / cyclomatic 256 / SLOC 4979 / functions 182.
- The new test function `run_sql_resource_with_ordered_cursor_commits_checkpoint` reports cognitive complexity 2, cyclomatic complexity 4, and SLOC 100.
- `crates/cdf-cli/src/commands.rs` remains mechanically split after the earlier architecture child: current metrics are SLOC 107, cognitive 2, cyclomatic 30.

CodeQL was not rerun for this test-only slice. The standing goal explicitly says to skip CodeQL unless the current change is complex/high-risk or part of a dedicated deep batch, and the reusable CodeQL wrapper would have had to recreate the database because Rust test source changed.

## What this supports

This supports the ticket acceptance criteria that a table-backed declarative Postgres SQL resource with a ratified ordered cursor runs successfully through the product-facing `cdf run` CLI path, routes through the general run spine, records a receipt and terminal run-ledger event, commits the checkpoint through the checkpoint store, and does not expose the resolved source DSN.

It also supports that the implementation gap was coverage/evidence rather than missing `run_command.rs` SQL dependency wiring.

## Limits

The live tests skip when neither `TEST_DATABASE_URL` nor local Postgres binaries are available, matching the existing local Postgres harness behavior.

`jscpd` reports broad pre-existing duplication in `crates/cdf-cli/src/tests.rs`. No separate refactor ticket was opened for this SQL-run slice because the duplicated patterns are test-fixture style and abstracting this new focused case would make the acceptance evidence harder to read without reducing product risk. The broader `commands.rs` architecture concern is already closed by `.10x/tickets/done/2026-07-07-cli-command-module-architecture.md`, and remaining CLI command-family decomposition/planner work is owned by `.10x/tickets/2026-07-07-cli-remaining-command-planners.md`.
