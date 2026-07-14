Status: recorded
Created: 2026-07-08
Updated: 2026-07-13
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws4a-error-envelope-foundation.md, .10x/specs/cli-error-experience-catalog.md

# P1 WS4A error envelope foundation evidence

## What was observed

`cdf-cli` now carries stable error codes and optional structured remediation in the centralized error envelope foundation:

- `crates/cdf-cli/src/error_catalog.rs` defines `CDF-CLI-USAGE`, `CDF-CLI-NOT-SUPPORTED`, and documented generic lower-layer mappings for every `cdf_kernel::ErrorKind`.
- `crates/cdf-cli/src/output.rs` adds `ErrorBody.code`, optional `ErrorBody.remediation`, `CliError.code`, and optional `CliError.remediation` without removing `kind`, `message`, `exit_code`, or `not_supported`.
- `CliError::usage` still exits 2.
- `CliError::not_supported` still exits 78.
- `From<CdfError> for CliError` keeps the prior exit-code taxonomy through the generic mapping.
- Human plain-text errors still include the original `error: <message>` and now append generic remediation text when available.

## Procedure

Required records and source were read before edits:

- `VISION.md`
- `QUALITY.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws4a-error-envelope-foundation.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws4-error-experience-catalog.md`
- `.10x/specs/cli-error-experience-catalog.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/decisions/superseded/cli-command-grammar-and-parser.md`
- `.10x/decisions/cli-design-language-and-renderer.md`
- `crates/cdf-cli/src/output.rs`
- `crates/cdf-cli/src/commands.rs`
- `crates/cdf-cli/src/args.rs`
- relevant current `crates/cdf-cli/src/tests.rs` error and helper sections

Commands run:

- `cargo fmt --all`: pass.
- `cargo test -p cdf-cli unknown_command_returns_usage_exit_code usage_error_human_output_keeps_message_and_adds_remediation not_supported_error_preserves_exit_code_and_json_compatibility generic_lower_layer_conversion_uses_documented_mapping --locked`: failed before execution because `cargo test` accepts one filter; rerun as separate focused tests.
- `cargo test -p cdf-cli unknown_command_returns_usage_exit_code --locked`: pass.
- `cargo test -p cdf-cli usage_error_human_output_keeps_message_and_adds_remediation --locked`: pass.
- `cargo test -p cdf-cli not_supported_error_preserves_exit_code_and_json_compatibility --locked`: pass.
- `cargo test -p cdf-cli generic_lower_layer_conversion_uses_documented_mapping --locked`: pass.
- `cargo fmt --all -- --check`: pass.
- `cargo metadata --format-version=1 --locked > reports/ai-quality/cargo-metadata.json`: pass.
- `cargo check -p cdf-cli --all-targets --locked`: pass.
- `cargo check -p cdf-cli --all-targets --all-features --locked`: pass.
- `cargo check -p cdf-cli --all-targets --no-default-features --locked`: pass.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: pass.
- `cargo clippy -p cdf-cli --all-targets --all-features --locked -- -D warnings`: pass.
- `cargo clippy -p cdf-cli --all-targets --no-default-features --locked -- -D warnings`: pass.
- `cargo test -p cdf-cli --locked`: pass; 175 library tests, 1 integration test, and 0 doctests passed.
- `cargo tree -p cdf-cli --locked > reports/ai-quality/cdf-cli-cargo-tree.txt`: pass.
- `cargo tree -p cdf-cli --locked -d > reports/ai-quality/cdf-cli-cargo-tree-duplicates.txt`: pass.
- `jscpd . --reporters json,console --output reports/ai-quality/jscpd --ignore "**/target/**,**/.git/**,**/reports/**"`: pass; repository-wide report found 654 existing clones, 7,462 duplicated lines, 6.1518% duplicated lines, and `newClones: 0`. `jq` check found 0 clones involving `crates/cdf-cli/src/error_catalog.rs`, `output.rs`, or `lib.rs`.
- `rust-code-analysis-cli -m -p crates/cdf-cli/src -O json -o reports/ai-quality/rust-code-analysis-cdf-cli`: pass after creating the output directory. Relevant metrics: `generic_lower_layer_mapping` cyclomatic 8 and cognitive 1; `CliError::usage`, `CliError::not_supported`, and `From<CdfError>` cyclomatic 1 and cognitive 0; `InvocationResult::from_error` cyclomatic 2 and cognitive 2; `format_remediation` cyclomatic 2 and cognitive 1.
- `tools/codeql-rust-quality.sh`: pass and wrote `target/quality/reports/codeql-rust-current.sarif`; SARIF contained 0 results. Limits: CodeQL diagnostics reported 246 Rust files extracted, 3753 extraction warnings, 192 files extracted with errors, and 54 without errors.
- `cargo deny check`: pass with existing duplicate dependency warnings; policy result ended `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo audit`: pass with 1 allowed warning for unmaintained `paste` (`RUSTSEC-2024-0436`).

## Parent review reruns

Parent review reran these checks after worker handoff:

- `cargo fmt --all -- --check`: passed.
- `cargo test -p cdf-cli --locked --no-fail-fast`: passed, 175 library tests, 1 integration test, and doc-tests.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- Semgrep Rust scan over `error_catalog.rs`, `output.rs`, `lib.rs`, and `tests.rs`: 0 findings.
- jscpd over `error_catalog.rs`, `output.rs`, `lib.rs`, and `tests.rs`: 24 clones, 337 duplicated lines, 4.01% duplicated lines. Clones are in the existing long CLI test surface, not in the new catalog module.
- `cargo deny --locked check advisories licenses sources`: passed.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: passed.
- `cargo vet --locked --no-minimize-exemptions`: passed.
- scc over touched Rust files: 4 files, 8,396 lines, 2,131 code lines, complexity 20.
- rust-code-analysis over touched Rust files: completed with reports under `target/quality/reports/ws4a-parent-rust-code-analysis/`.
- `tools/codeql-rust-quality.sh`: reused the fresh database at `target/quality/codeql-db-rust`; SARIF result count was `0`; extractor metrics were 246 files extracted, 0 extraction errors, 3753 extraction warnings, and 5797 unresolved macro calls.
- Scoped Gitleaks over touched source and WS4A records: no leaks.
- Direct unsafe/FFI/raw-pointer scan over touched Rust files: no matches.
- Forbidden phrase scan across `.10x`, docs, crates, root docs, changelog, tools, `.github`, and `LICENSE`: no matches.
- Scoped `git diff --check`: passed.

## What this supports or challenges

This supports all WS4A acceptance criteria:

- Stable additive JSON fields are present while existing fields remain.
- Generic lower-layer `CdfError` conversion is documented in source and covered by tests.
- Usage and not-supported exit codes remain 2 and 78.
- JSON compatibility, usage, not-supported, and generic lower-layer conversion tests pass.
- Human errors retain the previous message and add remediation rather than reducing information.

## Limits

WS4A intentionally did not migrate every `CliError` construction site to specific product codes; WS4B owns that. It did not implement suggestions, generated docs, or final renderer-integrated error presentation. CodeQL completed with no findings but with known Rust extractor quality limitations recorded above.
