Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-g1-source-diagnostics-and-deep-validate-foundation.md

# P2 WS-G1 source diagnostics partial verification

## What was observed

Worker G1 implemented the CLI-side source diagnostics foundation and reached partial verification. Formatting and whitespace checks for the G1 write scope pass. Rust compile/test verification is blocked by concurrent non-G1 `cdf-declarative/src/file_runtime.rs` edits.

Update 2026-07-09: This is historical partial evidence only. The concurrent compile blocker was later resolved by the E2/D lane, and G1 closure verification is recorded in `.10x/evidence/2026-07-09-p2-e2-g1-b4-batch.md`.

## Procedure

- `cargo check -p cdf-cli --tests --locked`
  - Result before later concurrent `cdf-declarative` edits: passed for the G1 implementation shape at that point.
  - Result after the concurrent edits appeared and after the final G1 scan/test additions: failed before G1 tests ran with:
    - initially `error[E0382]: borrow of moved value: matches` in `crates/cdf-declarative/src/file_runtime.rs:471`
    - latest rerun after the user-reported validate signature concern: `error[E0382]: borrow of partially moved value: metadata` in `crates/cdf-declarative/src/compiled.rs:230`
    - note: G1 did not edit this file.
- `rg -n "Command::Validate|pub\\(crate\\) fn validate|enum Command|ValidateArgs" crates/cdf-cli/src/args.rs crates/cdf-cli/src/commands.rs crates/cdf-cli/src/project_command.rs`
  - Result: confirmed the validate argument migration is complete: `Command::Validate(ValidateArgs)`, parser construction of `ValidateArgs { deep: ... }`, `commands.rs` dispatch with `args`, and `project_command::validate(&Cli, ValidateArgs)`.
- `cargo test -p cdf-cli validate_deep resource_not_compiled parser_provides_subcommand_help resource_mapping_pattern_mismatch --locked`
  - Result: command syntax error because Cargo accepts only one test-name filter.
- `cargo test -p cdf-cli validate_deep --locked`, `cargo test -p cdf-cli resource_not_compiled --locked`, `cargo test -p cdf-cli parser_provides_subcommand_help --locked`, and `cargo test -p cdf-cli resource_mapping_pattern_mismatch --locked`
  - Result: all blocked before test execution by the same non-G1 `cdf-declarative/src/file_runtime.rs` compile failures.
- `cargo fmt -p cdf-cli`
  - Result: passed and formatted the CLI package.
- `cargo fmt -p cdf-cli -- --check`
  - Result: passed.
- `git diff --check -- crates/cdf-cli/src/args.rs crates/cdf-cli/src/commands.rs crates/cdf-cli/src/context.rs crates/cdf-cli/src/error_catalog.rs crates/cdf-cli/src/project_command.rs crates/cdf-cli/src/scan_command.rs crates/cdf-cli/src/tests.rs .10x/tickets/done/2026-07-09-p2-ws-g1-source-diagnostics-and-deep-validate-foundation.md`
  - Result: passed.

## What this supports

This supports that the G1 patch is formatted and that the intended focused verification commands are known. It also supports the ticket blocker: current acceptance verification cannot complete until the concurrent `cdf-declarative` compile break is resolved by its owning lane.

## Limits

This evidence did not close G1 at the time it was recorded. Later closure evidence supersedes that blocker; keep this record only as an audit trail for the transient verification failure.
