Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-cli-run-resume-replay-inspect-spine.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# CLI run/resume/replay/inspect spine evidence

## What was observed

All executable children of the CLI spine parent are closed:

- `cdf run` general runtime wiring: `.10x/tickets/done/2026-07-07-cli-run-general-runtime.md`
- `cdf replay package` spine wiring: `.10x/tickets/done/2026-07-07-cli-replay-package-spine.md`
- `cdf resume` spine recovery: `.10x/tickets/done/2026-07-07-cli-resume-spine.md`
- `cdf inspect run` read-only ledger reporting: `.10x/tickets/done/2026-07-07-cli-inspect-run-spine.md`
- direct CLI table-backed SQL run success: `.10x/tickets/done/2026-07-07-cli-sql-run-success.md`

The earlier closure concern in `.10x/reviews/2026-07-07-cli-spine-parent-closure-audit.md` was specific to missing direct CLI SQL success evidence. That gap is closed by `.10x/evidence/2026-07-07-cli-sql-run-success.md`.

## Procedure

Parent integration commands run from `/Users/alexanderbut/code_projects/personal/firn`:

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo test -p cdf-cli run_sql_resource --locked -- --nocapture`: passed, 3 matched CLI SQL tests.
- `cargo test -p cdf-project general_project_run_executes_table_backed_postgres_sql_resource_stream --locked -- --nocapture`: passed.
- `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo test --workspace --locked --no-fail-fast`: passed across workspace unit tests and doc tests.
- `cargo deny check`: passed, with existing duplicate-version warnings only.
- `cargo audit`: passed with the already-ratified `paste` advisory warning.
- `cargo machete --with-metadata`: passed.
- `semgrep scan --no-git-ignore --config p/rust --config p/security-audit --json --output reports/ai-quality/semgrep-cli-sql-run-success.json crates/cdf-cli/src/tests.rs`: passed with 0 findings.
- `gitleaks dir --no-banner --redact --report-format json --report-path reports/ai-quality/gitleaks-cli-sql-run-success.json crates/cdf-cli/src/tests.rs`: passed with no leaks.
- `rust-code-analysis-cli -m -O json -p crates/cdf-cli/src > reports/ai-quality/rust-code-analysis-cli-sql-run-after-cli.json`: passed.
- `jscpd crates/cdf-cli/src --reporters json --output reports/ai-quality/jscpd-cli-sql-run-after-cli`: passed.
- `scc --format json --output reports/ai-quality/scc-cli-sql-run-after-cli.json crates/cdf-cli/src`: passed.

CodeQL was intentionally not rerun for this narrow test-only final slice under the active standing-goal quality note: skip CodeQL unless the current change is complex/high-risk or part of a dedicated deep batch. The local wrapper would recreate the database because Rust test source changed, which is exactly the expensive churn the user asked to avoid outside dedicated deep passes.

## What this supports

This supports the parent acceptance criteria:

- `cdf run` no longer rejects supported REST, SQL table, file, DuckDB, Parquet, or Postgres combinations solely because the runtime was specialized.
- `cdf resume` drains interrupted work through the run ledger and crash matrix without source contact after package finalization.
- `cdf replay package <pkg> --to <dest>` creates a run, records duplicate receipts, and preserves package replay determinism.
- `cdf inspect run <id>` shows run plan, verdict summaries, receipts, transitions, package/checkpoint pointers, duplicate status, and recovery guidance without leaking secrets.
- Stable JSON output is covered by the child CLI tests for automation-relevant fields.

## Limits

This evidence closes the CLI run/resume/replay/inspect spine parent only. It does not close the broader CLI surface parent `.10x/tickets/2026-07-05-cli-surface.md`; remaining non-run-spine command-family lower layers were later split by `.10x/tickets/done/2026-07-07-cli-remaining-command-planners.md`.

The final SQL success test uses the established live Postgres harness and skips when neither `TEST_DATABASE_URL` nor local Postgres binaries are available.
