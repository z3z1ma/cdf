Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/2026-07-08-p1-e5-trust-ring-ledger-events.md

# P1 E5 partial trust-ledger evidence

## What Was Observed

The partial E5 implementation adds `validation_depth_transition_recorded` run-ledger events and wires the project runtime to emit validation-depth transition evidence after package finalization and before checkpoint proposal.

The implemented live signals are:

- first contact: `discovery -> full` with trigger `new_resource`;
- promotion: `full -> sampled_fast_path` with trigger `clean_stable_runs` only when the current clean run crosses the compiled `clean_runs_required` count on a stable schema hash;
- demotion: `sampled_fast_path -> full` with trigger `drift` when the current schema hash differs from the prior committed head and `demote_on_drift` is set;
- demotion: `sampled_fast_path -> full` with trigger `quarantine_event` when the package contains quarantine artifacts and `demote_on_quarantine` is set.

Events carry resource/package/run pointers through existing ledger fields and details for `from_depth`, `to_depth`, `trigger`, `schema_hash`, optional `previous_schema_hash`, and clean-run counts. Event details are validated by the existing run-ledger redaction guard and do not advance checkpoint state.

The remaining E5 acceptance blocker is anomaly-spike demotion. Current runtime/package facts expose no anomaly-spike signal or ratified threshold, so implementing that trigger would invent product semantics.

## Procedure

Focused checks run against the partial implementation:

- `cargo fmt --all -- --check`: passed after formatting.
- `cargo check -p cdf-project -p cdf-state-sqlite -p cdf-cli --all-targets --locked`: passed.
- `cargo clippy -p cdf-state-sqlite -p cdf-project -p cdf-cli --all-targets --locked -- -D warnings`: passed after replacing a test `iter().any()` with `contains`.
- `cargo test -p cdf-state-sqlite run_ledger --locked`: passed, 9 tests.
- `cargo test -p cdf-project trust_ring_ --locked -- --nocapture`: passed, 3 tests.
- `cargo test -p cdf-project general_project_run_records_ledger_events_in_commit_gate_order --locked -- --nocapture`: passed.
- `cargo test -p cdf-cli run_local_file_to_duckdb_commits_package_rows_mirrors_and_checkpoint --locked -- --nocapture`: passed.
- `cargo test -p cdf-cli inspect_run_reports_completed_run_json_and_human --locked -- --nocapture`: passed.
- `cargo test -p cdf-cli run_sql_resource_with_ordered_cursor_commits_checkpoint --locked -- --nocapture`: passed.
- `jscpd crates/cdf-state-sqlite/src crates/cdf-project/src/runtime crates/cdf-project/src/runtime_tests.rs crates/cdf-cli/src/reports.rs crates/cdf-cli/src/tests.rs --reporters json,console --output target/quality/reports/jscpd-p1-e5-trust-ledger-partial --ignore "**/target/**,**/.git/**,**/reports/**"`: completed with a JSON report. The broad test-file scan reported 153 clones, 1,666 duplicated lines (10.15%), and 11,619 duplicated tokens (11.48%); this is a baseline signal for the touched broad test files, not a closure pass.
- `rust-code-analysis-cli -m -O json -p crates/cdf-project/src/runtime -p crates/cdf-state-sqlite/src -p crates/cdf-cli/src > target/quality/reports/rust-code-analysis-p1-e5-trust-ledger-partial.json`: passed.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-p1-e5-trust-ledger-partial.json crates/cdf-state-sqlite/src crates/cdf-project/src crates/cdf-cli/src`: passed, 0 findings.
- `semgrep scan --config p/security-audit --error --json --output target/quality/reports/semgrep-security-p1-e5-trust-ledger-partial.json crates/cdf-state-sqlite/src crates/cdf-project/src crates/cdf-cli/src`: passed, 0 findings.
- `gitleaks detect --no-git --source crates --report-format json --report-path target/quality/reports/gitleaks-p1-e5-trust-ledger-partial.json --no-banner --redact`: passed, no leaks.
- `rg -n "\bunsafe\b|extern \"|raw pointer|\*const|\*mut|unsafe impl|impl Send|impl Sync" crates/cdf-state-sqlite/src crates/cdf-project/src crates/cdf-cli/src`: no matches.

## What This Supports

This evidence supports committing the partial E5 implementation safely. It does not support closing E5 because anomaly-spike demotion is still unresolved.

## Limits

No closure review was performed because E5 is blocked, not done. Full E5 closure still requires anomaly-spike trigger semantics, any additional implementation needed for that signal, final quality gates, adversarial review, parent graph updates, and ticket closure.
