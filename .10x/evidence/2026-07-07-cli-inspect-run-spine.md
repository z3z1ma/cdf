Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-cli-inspect-run-spine.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# CLI inspect-run spine evidence

## What was observed

`cdf inspect run <id>` was implemented as a read-only CLI report over the SQLite run ledger. The command accepts and validates the run id, opens the selected environment ledger without initializing missing state, emits stable JSON fields for automation, includes ordered ledger events and artifact pointers, reports missing packages/receipts/checkpoints explicitly, redacts resolved secret-like details, and returns human output suitable for scheduler logs.

`SqliteRunLedger::open_read_only` was added so inspection can read an existing ledger without creating the database or schema. Tests cover missing read-only state, successful snapshots from an existing database, failed writes through a read-only handle, and future schema-version rejection for both mutating and read-only opens.

The implementation was performed by worker lane `019f3e7e-08d0-7ab3-b0ec-4c0a55882ef0` and reconciled by the parent agent.

## Procedure

Quality and verification commands run after implementation:

```text
cargo fmt --all -- --check
git diff --check
cargo check -p cdf-cli --locked
cargo check -p cdf-state-sqlite --locked
cargo check --workspace --all-targets --locked
cargo clippy -p cdf-cli -p cdf-state-sqlite --all-targets --locked -- -D warnings
cargo test -p cdf-cli inspect_run --locked -- --nocapture
cargo test -p cdf-state-sqlite sqlite_run_ledger --locked -- --nocapture
cargo test -p cdf-cli --locked --no-fail-fast
cargo test -p cdf-state-sqlite --locked --no-fail-fast
cargo test --workspace --locked --no-fail-fast
semgrep scan --no-git-ignore --config p/rust --json --output reports/ai-quality/semgrep-cli-inspect-run.json crates/cdf-cli/src crates/cdf-state-sqlite/src
gitleaks dir --no-banner --redact --report-format json --report-path reports/ai-quality/gitleaks-cli-inspect-run-cli.json crates/cdf-cli/src
gitleaks dir --no-banner --redact --report-format json --report-path reports/ai-quality/gitleaks-cli-inspect-run-state.json crates/cdf-state-sqlite/src
rust-code-analysis-cli -m -O json -p crates/cdf-cli/src > reports/ai-quality/rust-code-analysis-cli-inspect-run-before.json
rust-code-analysis-cli -m -O json -p crates/cdf-cli/src > reports/ai-quality/rust-code-analysis-cli-inspect-run-after-cli.json
rust-code-analysis-cli -m -O json -p crates/cdf-state-sqlite/src > reports/ai-quality/rust-code-analysis-cli-inspect-run-after-state.json
jscpd crates/cdf-cli/src --reporters json --output reports/ai-quality/jscpd-cli-inspect-run-before
jscpd crates/cdf-cli/src --reporters json --output reports/ai-quality/jscpd-cli-inspect-run-after-cli
jscpd crates/cdf-cli/src crates/cdf-state-sqlite/src --reporters json --output reports/ai-quality/jscpd-cli-inspect-run-after
scc --format json --output reports/ai-quality/scc-cli-inspect-run-after-cli.json crates/cdf-cli/src
scc --format json --output reports/ai-quality/scc-cli-inspect-run-after-state.json crates/cdf-state-sqlite/src
```

The first combined `gitleaks` command over both source trees hung for more than three minutes and was interrupted. It was replaced by the split CLI and state scans above, both of which completed cleanly.

CodeQL was intentionally not recreated for this slice per the project instruction to keep reusable CodeQL databases instead of rebuilding them during normal quality checks. `cargo deny` and `cargo audit` were not rerun because this ticket did not change dependencies or lockfiles.

## Results

All cargo format/check/clippy/test commands above passed. Full workspace tests passed across all workspace crates and doc tests.

Focused test results:

- `cdf-cli inspect_run`: 6 passed.
- `cdf-state-sqlite sqlite_run_ledger`: 9 passed.
- Full `cdf-cli`: 93 library tests, 1 integration test, and doc tests passed.
- Full `cdf-state-sqlite`: 27 tests and doc tests passed.

Security scans:

- Semgrep `p/rust`: 0 findings across 33 scanned files and 11 rules.
- Gitleaks CLI source scan: no leaks.
- Gitleaks state source scan: no leaks.

Duplicate and complexity metrics:

- CLI-only `jscpd` before: 26 sources, 9,394 lines, 73 clones, 714 duplicated lines, 7.6006 percent duplicated lines, 5,298 duplicated tokens, 9.0727 percent duplicated tokens.
- CLI-only `jscpd` after: 27 sources, 10,383 lines, 77 clones, 759 duplicated lines, 7.3100 percent duplicated lines, 5,574 duplicated tokens, 8.6227 percent duplicated tokens.
- Combined CLI plus state `jscpd` after: 33 sources, 13,549 lines, 88 clones, 901 duplicated lines, 6.6499 percent duplicated lines, 6,525 duplicated tokens, 7.7696 percent duplicated tokens.
- `inspect_command.rs` before: SLOC 111, PLOC 104, cyclomatic 18, cognitive 3, functions 4.
- `inspect_command.rs` after: SLOC 112, PLOC 105, cyclomatic 19, cognitive 3, functions 4.
- New `inspect_run_command.rs`: SLOC 701, PLOC 661, cyclomatic 141, cognitive 90, functions 22.
- `run_ledger.rs` after: SLOC 781, PLOC 633, cyclomatic 218, cognitive 28, functions 41. No pre-change state-crate baseline was captured for this slice.

## What this supports

This supports the ticket acceptance criteria for parser coverage, stable JSON output, ordered ledger event inspection, package/receipt/checkpoint pointer reporting, explicit missing-artifact statuses, duplicate-status reporting, redacted details, and no-write inspection behavior.

It also supports the architectural cleanup requested by the user: `commands.rs` stays as a dispatcher and inspect-run behavior lives in a dedicated command module instead of growing the central CLI command file.

## Limits

Checkpoint availability is ledger-derived in this slice. It reports proposed/committed/not-recorded status from run events and does not verify a checkpoint table row. Resume mutation and checkpoint repair remain explicitly excluded and owned by the open CLI resume spine work.

The new inspect-run module is contained but sizeable. The complexity metrics above should be treated as a watch item for the next observability cleanup pass rather than evidence of a cross-command `commands.rs` regression.
