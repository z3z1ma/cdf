Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-cli-command-module-architecture.md, .10x/specs/project-cli-observability-security.md

# CLI command module architecture evidence

## What was observed

`cdf-cli` command implementation was split mechanically by command family. `crates/cdf-cli/src/commands.rs` now acts as the command dispatcher plus shared output/error helpers. Command-family implementation moved into focused modules:

- `backfill_command.rs`
- `contract_command.rs`
- `doctor_command.rs`
- `inspect_command.rs`
- `package_command.rs`
- `project_command.rs`
- `replay_command.rs`
- `resume_command.rs`
- `scan_command.rs`
- `sql_command.rs`
- `state_command.rs`
- `status_command.rs`

Shared run/replay report serialization moved into `reports.rs`. `run_command.rs` now imports shared reports from `reports.rs` and scan plan construction from `scan_command.rs`.

## Procedure

Quality gates run from `/Users/alexanderbut/code_projects/personal/firn`:

- `cargo fmt --all -- --check` passed.
- `cargo check -p cdf-cli --locked` passed.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings` passed.
- `cargo test -p cdf-cli --locked --no-fail-fast` passed: 87 library tests, 0 binary tests, 1 integration test, 0 doc tests.
- `cargo check --workspace --all-targets --locked` passed.
- `git diff --check` passed.
- `rust-code-analysis-cli -m -O json -p crates/cdf-cli/src` generated `reports/ai-quality/rust-code-analysis-cli-command-architecture-after.json`.
- `jscpd crates/cdf-cli/src --reporters json --output reports/ai-quality/jscpd-cli-command-architecture-after` generated `reports/ai-quality/jscpd-cli-command-architecture-after/jscpd-report.json`.
- `scc --by-file --format json crates/cdf-cli/src` generated `reports/ai-quality/scc-cli-command-architecture-after.json`.
- `semgrep scan --no-git-ignore --config p/rust --json --output reports/ai-quality/semgrep-cli-command-architecture.json crates/cdf-cli/src` passed with 0 findings across 26 files and 11 Rust rules.
- `gitleaks dir --no-banner --redact --report-format json --report-path reports/ai-quality/gitleaks-cli-command-architecture.json crates/cdf-cli/src` passed with no leaks found.

## Metrics

`rust-code-analysis-cli` for `crates/cdf-cli/src/commands.rs`:

| Metric | Before | After |
| --- | ---: | ---: |
| SLOC | 2078 | 107 |
| PLOC | 1898 | 72 |
| Cyclomatic sum | 370 | 30 |
| Cognitive sum | 85 | 2 |
| Functions | 84 | 5 |
| Closures | 28 | 0 |

`jscpd` over `crates/cdf-cli/src`:

| Metric | Before | After |
| --- | ---: | ---: |
| Sources | 13 | 26 |
| Lines | 9273 | 9394 |
| Clones | 73 | 73 |
| Duplicated lines | 714 | 714 |
| Duplicated line percentage | 7.70% | 7.60% |
| Duplicated tokens | 5298 | 5298 |
| Duplicated token percentage | 9.21% | 9.07% |

`scc` after metrics over `crates/cdf-cli/src`:

- Total Rust files: 26.
- Total lines: 9394.
- Total code lines: 5409.
- Total complexity: 332.
- `commands.rs`: 107 lines, 99 code lines, complexity 2.

## What this supports or challenges

This supports the ticket acceptance criteria:

- `commands.rs` no longer contains dominant implementation bodies for unrelated command families.
- Command-family helpers and report structs moved to noun-owned modules where they are not shared.
- Shared report/output code is explicitly named in `commands.rs` and `reports.rs`.
- The behavior-preserving boundary is supported by the existing CLI test suite, clippy, workspace check, and static/security scans.
- Duplication did not increase by clone count, duplicated lines, or duplicated tokens.

## Limits

This evidence does not claim new command semantics. It does not close the remaining lower-layer command planner tickets for `init`, `preview`, `contract`, `state migrate/recover`, `backfill`, `package gc`, or remaining `status` behavior. CodeQL was not rerun locally because this architecture-only slice did not change dependencies, unsafe code, or generated CodeQL-relevant security behavior, and the user explicitly requested avoiding expensive local CodeQL database recreation.
