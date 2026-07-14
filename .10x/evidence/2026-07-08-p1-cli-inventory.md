Status: recorded
Created: 2026-07-08
Updated: 2026-07-13
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws2a-cli-grammar-decision.md, .10x/tickets/done/2026-07-08-p1-product-ws3a-cli-design-language-decision.md

# P1 CLI inventory evidence

## What was observed

The current `cdf-cli` grammar and renderer posture were inspected before ratifying the WS2 and WS3 decisions.

Key observations:

- `crates/cdf-cli/src/args.rs` owns a hand-rolled parser with `Cli`, `Command`, argument structs, `parse_command`, and per-command parse helpers.
- `crates/cdf-cli/src/commands.rs` owns static global help text and dispatch.
- `crates/cdf-cli/src/output.rs` owns the stable JSON and human output envelope. `CommandOutput` carries `human: String` and `json: serde_json::Value`.
- `crates/cdf-cli/src/reports.rs` has raw `human_message()` strings for run and replay reports.
- `crates/cdf-cli/src/scan_command.rs` has `format_scan_report()` plus direct preview string formatting.
- Many other command modules directly format human strings.
- Current JSON error output includes `kind`, `message`, `exit_code`, and `not_supported`; success output includes `ok`, `command`, and `result`.
- `Cargo.lock` already contains `clap`, `comfy-table`, `crossterm`, `anstyle`, and `is-terminal` through current workspace dependencies; `cdf-cli` does not yet depend on parser/renderer crates.

## Procedure

Read-only commands included:

```text
sed -n '1,260p' crates/cdf-cli/src/args.rs
sed -n '1,180p' crates/cdf-cli/src/commands.rs
sed -n '1,160p' crates/cdf-cli/src/output.rs
sed -n '1,180p' crates/cdf-cli/src/reports.rs
sed -n '1,220p' crates/cdf-cli/src/run_command.rs
sed -n '1,260p' crates/cdf-cli/src/scan_command.rs
rg -n "enum Command|struct .*Args|fn parse_|CommandOutput|human:|human_message|format_scan_report|--json|--project|--env|exit_code|not_supported" crates/cdf-cli/src -g '*.rs'
rg -n "name = \"(clap|clap_builder|clap_complete|clap_mangen|comfy-table|console|owo-colors|indicatif|insta|anstyle|anstream|crossterm|is-terminal)\"|\\[\\[package\\]\\]" Cargo.lock
```

## What this supports or challenges

This supported `.10x/decisions/superseded/cli-command-grammar-and-parser.md` and `.10x/decisions/cli-design-language-and-renderer.md` by grounding them in the then-current source layout and compatibility surface.

## Limits

This is source inventory, not implementation evidence. It does not prove parser migration, rendering quality, generated completions, man pages, snapshots, or live progress.
