Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-cli-resume-spine.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# CLI resume spine evidence

## What was observed

`cdf resume --run <id>` and positional `cdf resume <id>` now recover a referenced run from the selected environment's run ledger, package artifacts, durable receipts, and checkpoint rows.

Supported outcomes:

- Terminal successful runs return stable no-op JSON and append `run_resumed`.
- No finalized package, missing package artifact, inconsistent package artifacts, and inconsistent checkpoint/receipt facts fail closed with recovery guidance and append `run_failed` when possible.
- Finalized package with no durable receipt replays the package without source contact for DuckDB, filesystem Parquet, and Postgres. Postgres derives target from durable package replay inputs and requires explicit selected-environment policy `merge_dedup = "fail"` before replay.
- Durable receipt with uncommitted checkpoint verifies the receipt through existing project recovery primitives before checkpoint commit.
- Committed checkpoint with stale package status updates package status only after proving the current committed head is exact: status committed, `is_head`, same `StateDelta`, and same selected durable receipt.

The implementation is split under `crates/cdf-cli/src/resume_command/`:

- `resume_command.rs`: thin command entrypoint.
- `attempt.rs`: recovery decision flow and mutation calls.
- `destination.rs`: destination selection, Postgres policy mapping, and redaction.
- `events.rs`: run-ledger event assembly.
- `model.rs`: package/checkpoint/receipt facts and exact-head proof.
- `report.rs`: stable JSON report structs.

`crates/cdf-cli/src/commands.rs` remained unchanged by this child.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

- `cargo test -p cdf-cli resume --locked -- --nocapture`: passed, 9 resume tests.
- `cargo test -p cdf-cli --locked --no-fail-fast`: passed, 102 library tests, 1 integration test, and doc-tests.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo test --workspace --locked --no-fail-fast`: passed before the final CLI-only split; this covered unchanged `cdf-project` recovery tests including DuckDB, Parquet, and Postgres package replay/recovery.
- `semgrep scan --no-git-ignore --config p/rust --json --output reports/ai-quality/semgrep-cli-resume.json crates/cdf-cli/src`: passed; 0 findings across 32 files.
- `gitleaks dir --no-banner --redact --report-format json --report-path reports/ai-quality/gitleaks-cli-resume-cli.json crates/cdf-cli/src`: passed; no leaks.
- `jscpd crates/cdf-cli/src --reporters json --output reports/ai-quality/jscpd-cli-resume-after-cli`: passed; JSON report written.
- `rust-code-analysis-cli -m -O json -p crates/cdf-cli/src > reports/ai-quality/rust-code-analysis-cli-resume-after-cli.json`: passed.
- `scc --format json --output reports/ai-quality/scc-cli-resume-after-cli.json crates/cdf-cli/src`: passed.

Quality metric snapshots:

- `jscpd` before: 27 sources, 10,383 lines, 77 clones, 759 duplicated lines, 7.3100% duplicated lines, 8.6227% duplicated tokens.
- `jscpd` after: 32 sources, 12,017 lines, 92 clones, 890 duplicated lines, 7.4062% duplicated lines, 8.6720% duplicated tokens.
- `rust-code-analysis` for `commands.rs` before and after: cognitive 2, cyclomatic 30, 107 SLOC, 5 functions.
- `rust-code-analysis` for final resume modules:
  - `resume_command.rs`: cognitive 2, cyclomatic 15, 48 SLOC, 1 function.
  - `resume_command/attempt.rs`: cognitive 22, cyclomatic 117, 651 SLOC, 17 functions.
  - `resume_command/destination.rs`: cognitive 5, cyclomatic 16, 93 SLOC, 3 functions.
  - `resume_command/events.rs`: cognitive 3, cyclomatic 14, 79 SLOC, 5 functions.
  - `resume_command/model.rs`: cognitive 16, cyclomatic 34, 163 SLOC, 7 functions.
  - `resume_command/report.rs`: cognitive 4, cyclomatic 20, 141 SLOC, 7 functions.
- Highest final resume function in the CLI top-15 complexity report: `resume_command/attempt.rs::execute`, cognitive 8, cyclomatic 18, 96 SLOC.
- `scc` after for `crates/cdf-cli/src`: 32 Rust files, 12,017 lines, 7,170 code lines, complexity 487.

CodeQL was intentionally not run for this slice per active user/goal instruction to avoid recreating or churning the reusable database. Dependency gates such as `cargo deny` and `cargo audit` were not rerun because this slice did not change Cargo manifests or `Cargo.lock`.

## What this supports

This evidence supports the resume child acceptance criteria for run-id-scoped recovery, no source contact after package finalization, durable receipt verification before checkpoint commit, status-only repair after committed checkpoint, terminal no-op output, fail-closed missing/inconsistent evidence, and command-module architecture discipline.

It also supports the user concern that `commands.rs` must not absorb another vertical slice: `commands.rs` did not change, and resume internals are split by concern.

## Limits

This evidence closes the resume child only. During parent closure audit, the broader CLI run/resume/replay/inspect parent remained open because CLI table-backed SQL run success still lacks a direct CLI success-path owner/evidence even though lower `cdf-project` SQL source execution exists.
