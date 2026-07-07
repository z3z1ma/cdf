Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-cli-run-general-runtime.md, .10x/tickets/done/2026-07-07-cli-command-module-architecture.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# CLI REST/Postgres run quality evidence

## What was observed

The `cdf run` CLI path now delegates run execution out of `crates/cdf-cli/src/commands.rs` into `crates/cdf-cli/src/run_command.rs`, with destination URI/redaction helpers in `crates/cdf-cli/src/destination_uri.rs` and the production REST transport in `crates/cdf-cli/src/http_transport.rs`.

The run path now constructs `ProjectRunRequest` for:

- local file resources to DuckDB, filesystem Parquet, and Postgres destinations;
- exact zero-lag declarative REST resources using a production `ReqwestHttpTransport`;
- table-backed Postgres SQL resources using the project secret provider.

Postgres `cdf run` destination execution is fail-closed unless the selected environment includes `[environments.<name>.destination_policy.postgres] merge_dedup = "fail"`. The policy is parsed as typed project configuration in `cdf-project` and overlays from the default environment into named environments.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

- `cargo fmt --all`: passed.
- `cargo check -p cdf-cli --locked`: passed after removing extraction leftovers.
- `cargo test -p cdf-project destination_policy --locked --no-fail-fast`: passed, 1 test.
- `cargo test -p cdf-cli run_rest_resource --locked --no-fail-fast`: passed, 2 tests.
- `cargo test -p cdf-cli run_postgres_destination --locked --no-fail-fast`: passed, 4 tests, including live local Postgres.
- `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo fmt --all -- --check && git diff --check`: passed.
- `cargo test --workspace --locked --no-fail-fast`: passed across workspace unit tests and doc tests, including live local Postgres destination/project cases.
- `cargo deny check`: passed with existing duplicate-version warnings and final `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo audit`: passed with only the already-allowed `RUSTSEC-2024-0436` warning for `paste`.
- `cargo machete --with-metadata`: passed; no unused dependencies.
- `semgrep scan --config p/rust --error --json --output reports/ai-quality/semgrep-cli-run-split-explicit.json crates/cdf-cli/src/commands.rs crates/cdf-cli/src/run_command.rs crates/cdf-cli/src/destination_uri.rs crates/cdf-cli/src/http_transport.rs crates/cdf-cli/src/tests.rs crates/cdf-project/src/models.rs crates/cdf-project/src/tests.rs`: passed; 0 findings across 7 explicit touched files.
- `rg -n "\bunsafe\b|raw pointer|Send for|Sync for" crates/cdf-cli/src crates/cdf-project/src .10x/tickets/done/2026-07-07-cli-run-general-runtime.md || true`: no matches.
- `gitleaks dir --redact --report-format json --report-path reports/ai-quality/gitleaks-cdf-cli-run-split.json crates/cdf-cli`: passed; no leaks found.
- `gitleaks dir --redact --report-format json --report-path reports/ai-quality/gitleaks-cdf-project-run-split.json crates/cdf-project`: passed; no leaks found.

Maintainability metrics:

- `rust-code-analysis-cli -m -O json` before/after reports were written to `reports/ai-quality/rust-code-analysis-cli-before-run-split.json` and `reports/ai-quality/rust-code-analysis-cli-after-run-split.json`.
- Before this split, `commands.rs` reported 2414 SLOC, cyclomatic sum 461, cognitive sum 108.
- After this split, `commands.rs` reported 2078 SLOC, cyclomatic sum 370, cognitive sum 85.
- New run-specific modules measured separately: `run_command.rs` 226 lines, `destination_uri.rs` 69 lines, `http_transport.rs` 62 lines.
- `jscpd` before/after reports were written to `reports/ai-quality/jscpd-before-run-split/jscpd-report.json` and `reports/ai-quality/jscpd-after-run-split/jscpd-report.json`.
- `jscpd` before: 150 clones, 1686 duplicated lines, 9.8758% duplicated lines, 11.2033% duplicated tokens across 19 sources.
- `jscpd` after: 150 clones, 1686 duplicated lines, 9.8631% duplicated lines, 11.1784% duplicated tokens across 22 sources.
- `scc --format json crates/cdf-cli/src crates/cdf-project/src` wrote `reports/ai-quality/scc-cli-project-after-run-split.json`.

## What this supports

This evidence supports the `cdf run` child ticket acceptance criteria for REST, SQL, local-file, DuckDB, Parquet, and Postgres CLI routing through the general runtime with fail-closed unsupported cases, secret redaction, receipt-gated checkpoint commits, and stable JSON run reports.

It also supports the architectural claim that this slice reduced `commands.rs` concentration instead of adding another vertical implementation directly inside it.

## Limits

`commands.rs` remains too large and still owns unrelated command families. This evidence supports the run extraction only. The remaining CLI module split was later closed by `.10x/tickets/done/2026-07-07-cli-command-module-architecture.md`.

CodeQL was intentionally not run for this slice per the active goal/user instruction to avoid recreating or churning the reusable database for low-value local runs.
