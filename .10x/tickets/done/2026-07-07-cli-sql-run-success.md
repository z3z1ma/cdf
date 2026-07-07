Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/done/2026-07-07-cli-run-resume-replay-inspect-spine.md
Depends-On: .10x/tickets/done/2026-07-07-declarative-postgres-sql-resource-execution.md, .10x/tickets/done/2026-07-07-general-run-non-file-resource-streams.md, .10x/tickets/done/2026-07-07-cli-run-general-runtime.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# Prove CLI table-backed SQL run success

## Scope

Close the remaining CLI-spine parent gap for table-backed Postgres SQL resources by making `cdf run` execute a supported SQL resource through the general run spine from the CLI surface, with direct CLI success-path evidence.

Owns:

- `crates/cdf-cli/src/run_command.rs` and adjacent CLI runtime-dependency assembly if needed.
- `crates/cdf-cli/src/tests.rs` focused CLI SQL-run tests.
- CLI-facing project/runtime adapter changes only if the existing lower-layer SQL runtime dependencies cannot be supplied from the CLI without widening semantics.

## Acceptance criteria

- A table-backed declarative Postgres SQL resource with a ratified ordered cursor runs successfully through `cdf run` from the CLI.
- The run goes through `cdf_project::run_project`, writes a package, records run-ledger events, records a destination receipt, commits the checkpoint through `CheckpointStore::commit`, and emits stable JSON fields consistent with other `run` reports.
- The test proves source credentials are resolved through secret references without leaking resolved DSNs.
- Existing fail-closed SQL cases for missing secret and unsupported cursor/query shapes remain intact.

## Evidence expectations

Run focused CLI SQL-run success and failure tests, relevant `cdf-project` SQL runtime tests, `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`, `cargo check --workspace --all-targets --locked`, `git diff --check`, and applicable `QUALITY.md` security/duplication/complexity checks for touched files.

## Explicit exclusions

No arbitrary SQL query-resource execution beyond the already-ratified table-backed SQL resource slice, no new SQL dialects, no destination semantics changes, no scheduler/daemon work, and no lower-layer SQL runtime rewrite unless a concrete CLI integration blocker requires a narrow adapter.

## Design notes

Lower-layer table-backed Postgres SQL source execution is already closed under `.10x/tickets/done/2026-07-07-declarative-postgres-sql-resource-execution.md` and `.10x/tickets/done/2026-07-07-general-run-non-file-resource-streams.md`. The remaining gap is product-facing CLI success-path wiring/evidence, not source engine semantics.

## Evidence

- `.10x/evidence/2026-07-07-cli-sql-run-success.md`

## Review

- `.10x/reviews/2026-07-07-cli-sql-run-success-review.md`

## Blockers

None.

## Progress and notes

- 2026-07-07: Opened during CLI spine parent closure audit after resume closure. Audit found lower SQL runtime evidence but no direct CLI table-backed SQL `run` success-path test; existing CLI SQL tests prove only fail-closed missing secret and ordered-cursor validation behavior.
- 2026-07-07: Activated for worker execution. Parent inspection found `crates/cdf-cli/src/run_command.rs` already builds `SqlResource` with `SqlRuntimeDependencies::new().with_secret_provider(context.secret_provider())`; the likely gap is a live CLI SQL success fixture with an ordered cursor, not a new lower-layer SQL runtime.
- 2026-07-07: Added the focused live CLI SQL success test in `crates/cdf-cli/src/tests.rs`; no `run_command.rs` change was needed. The test creates a live Postgres source table, stores the resolved source DSN behind `secret://file/sql-dsn`, runs `cdf run` through the CLI helper, asserts stable JSON success fields, checks DSN/secret redaction, verifies DuckDB destination rows, and verifies the committed checkpoint head cursor at `updated_at = 20`.
- 2026-07-07: Verification passed: `cargo fmt --all -- --check`; `cargo test -p cdf-cli run_sql_resource --locked -- --nocapture`; `cargo test -p cdf-project general_project_run_executes_table_backed_postgres_sql_resource_stream --locked -- --nocapture`; `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`; `cargo check --workspace --all-targets --locked`; `git diff --check`; direct touched-file unsafe/FFI/raw-pointer search; Semgrep `p/rust` plus `p/security-audit` on `crates/cdf-cli/src/tests.rs`; Gitleaks on `crates/cdf-cli/src/tests.rs`; Rust-code-analysis metrics on `crates/cdf-cli/src/tests.rs`; and Jscpd on `crates/cdf-cli/src/tests.rs`. Evidence recorded in `.10x/evidence/2026-07-07-cli-sql-run-success.md`; review passed in `.10x/reviews/2026-07-07-cli-sql-run-success-review.md`.
- 2026-07-07: Parent integration reran broader gates and metrics: full workspace tests, workspace check, deny/audit/machete, Semgrep/Gitleaks over the touched CLI test file, and `rust-code-analysis-cli`/Jscpd/SCC over all `crates/cdf-cli/src`. CodeQL was intentionally skipped for this narrow test-only slice under the active standing-goal note that CodeQL is low value and expensive unless a change is high-risk or part of a dedicated deep batch.
