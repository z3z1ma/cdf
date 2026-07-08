Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws4c-error-suggestions.md, .10x/specs/cli-error-experience-catalog.md, .10x/decisions/cli-command-grammar-and-parser.md

# P1 WS4C error suggestions evidence

## What was observed

`cdf-cli` now emits additive JSON `error.suggestions` for high-confidence, redaction-safe error suggestions:

- Unknown top-level commands and known command subcommands attach the single nearest command path, such as `cdf status` or `cdf inspect resource`.
- Low-confidence command suggestions are omitted; `cdf --json bogus` retains no `error.suggestions` field.
- Unknown compiled project resource ids attach nearest configured resource ids from the loaded project inventory.
- Resource suggestions are omitted when there is no compiled resource inventory.
- Unsupported or malformed destination errors attach bounded URI-shape suggestions and, when the user appears to have supplied an environment name typo to `--to`, the nearest configured environment selector such as `--env prod`.
- Destination suggestion output does not include resolved secret values or userinfo-bearing destination credentials.
- Human plain-text errors can display a simple `suggestions:` block before WS4D renderer integration; JSON remains the authoritative compatibility surface for this ticket.

Changed source files:

- `crates/cdf-cli/src/output.rs`
- `crates/cdf-cli/src/suggestions.rs`
- `crates/cdf-cli/src/args.rs`
- `crates/cdf-cli/src/context.rs`
- `crates/cdf-cli/src/destination_uri.rs`
- `crates/cdf-cli/src/scan_command.rs`
- `crates/cdf-cli/src/run_command.rs`
- `crates/cdf-cli/src/backfill_command.rs`
- `crates/cdf-cli/src/replay_command.rs`
- `crates/cdf-cli/src/resume_command/destination.rs`
- `crates/cdf-cli/src/lib.rs`
- `crates/cdf-cli/src/tests.rs`

## Procedure

Records read before implementation:

- `.10x/tickets/done/2026-07-08-p1-product-ws4c-error-suggestions.md` (read while open before implementation, then moved to done at closure)
- `.10x/specs/cli-error-experience-catalog.md`
- `.10x/decisions/cli-command-grammar-and-parser.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws4a-error-envelope-foundation.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws4b-error-construction-site-migration.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws2c-product-grammar-semantics.md`
- `.10x/evidence/2026-07-08-p1-product-ws4a-error-envelope-foundation.md`
- `.10x/evidence/2026-07-08-p1-product-ws4b-error-construction-site-migration.md`
- `.10x/evidence/2026-07-08-p1-product-ws2c-product-grammar-semantics.md`
- `.10x/reviews/2026-07-08-p1-product-ws4a-error-envelope-foundation-review.md`
- `.10x/reviews/2026-07-08-p1-product-ws4b-error-construction-site-migration-review.md`
- `.10x/reviews/2026-07-08-p1-product-ws2c-product-grammar-semantics-review.md`
- `.10x/evidence/2026-07-08-p1-product-ws4-error-catalog-shaping.md`
- `.10x/reviews/2026-07-08-p1-product-ws4-error-catalog-shaping-review.md`
- `QUALITY.md` relevant Rust gate sections.

Focused tests:

- `cargo test -p cdf-cli unknown_command_and_subcommand_json_suggest_high_confidence_matches --locked`: initially failed because `staus` suggested both `cdf status` and `cdf state`; fixed by limiting command suggestions to the single nearest command. Final rerun passed.
- `cargo test -p cdf-cli unknown_resource_json_suggests_nearest_configured_resource_id --locked`: passed.
- `cargo test -p cdf-cli unknown_resource_json_omits_suggestions_without_inventory --locked`: passed.
- `cargo test -p cdf-cli unknown_destination_json_suggests_environment_or_uri_shape_without_secrets --locked`: passed.
- `cargo test -p cdf-cli unknown_command_returns_usage_exit_code --locked`: passed and proves low-confidence `bogus` omits suggestions.

Package and Rust gates:

- `cargo fmt --all`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo check -p cdf-cli --all-targets --locked`: passed.
- `cargo check -p cdf-cli --all-targets --all-features --locked`: passed.
- `cargo check -p cdf-cli --all-targets --no-default-features --locked`: passed.
- `cargo test -p cdf-cli --locked --no-fail-fast`: passed; 181 library tests, 1 integration test, and 0 doctests passed.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: initially failed after adding `Vec<String>` to `CliError` because `clippy::result_large_err` crossed the size threshold across many `CliError` return paths; fixed by storing suggestions as `Box<[String]>` in `CliError` while keeping JSON as `Vec<String>`. Final rerun passed.
- `cargo clippy -p cdf-cli --all-targets --all-features --locked -- -D warnings`: passed.
- `cargo clippy -p cdf-cli --all-targets --no-default-features --locked -- -D warnings`: passed.
- `cargo metadata --format-version=1 --locked > reports/ai-quality/cargo-metadata.json`: passed.
- `cargo tree -p cdf-cli --locked > reports/ai-quality/ws4c-cdf-cli-cargo-tree.txt`: passed.
- `cargo tree -p cdf-cli --locked -d > reports/ai-quality/ws4c-cdf-cli-cargo-tree-duplicates.txt`: passed.
- `git diff --check -- <touched WS4C source and ticket>`: passed.
- Direct unsafe-token scan over touched source files: no matches; `rg` exited 1 because no matches were found.

Quality, security, and supply-chain gates:

- `semgrep scan --config p/rust --error --json --output reports/ai-quality/semgrep-ws4c-rust.json crates/cdf-cli/src`: passed; 45 tracked files scanned, 11 Rust rules run, 0 findings.
- `jscpd . --reporters json,console --output reports/ai-quality/jscpd-ws4c --ignore "**/target/**,**/.git/**,**/reports/**"`: passed; repository-wide report found 657 existing clones, 7,507 duplicated lines, 6.140996695134322 percent duplication, and `newClones: 0`.
- `rust-code-analysis-cli -m -p crates/cdf-cli/src -O json -o reports/ai-quality/rust-code-analysis-ws4c`: initial run failed because the output path had to be a pre-existing directory; rerun after `mkdir -p reports/ai-quality/rust-code-analysis-ws4c` passed. `suggestions.rs` metrics include cognitive sum 8 and cyclomatic sum 8 across the helper and closures.
- `gitleaks dir --no-banner --redact crates/cdf-cli/src`: passed; no leaks found.
- `gitleaks dir --no-banner --redact .10x/tickets/done/2026-07-08-p1-product-ws4c-error-suggestions.md`: passed; no leaks found.
- `gitleaks dir --no-banner --redact .10x/evidence/2026-07-08-p1-product-ws4c-error-suggestions.md`: passed; no leaks found.
- `gitleaks dir --no-banner --redact .10x/reviews/2026-07-08-p1-product-ws4c-error-suggestions-review.md`: passed; no leaks found.
- `cargo deny --locked check advisories licenses sources`: passed.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: passed under the existing ratified `paste` advisory exception.
- `cargo vet --locked --no-minimize-exemptions`: passed; 455 exempted.
- `tools/codeql-rust-quality.sh --database target/quality/codeql-db-rust`: passed; the helper refreshed the database because Rust source changed.
- `jq '[.runs[].results[]?] | length' target/quality/reports/codeql-rust-current.sarif`: `0`.

Record graph hygiene after closure:

- `rg -n "\.10x/tickets/2026-07-08-p1-product-ws4c-error-suggestions\.md" .10x`: no matches; `rg` exited 1 because the former active ticket path no longer appears.
- `git diff --check -- .10x/tickets/2026-07-08-p1-product-ws4-error-experience-catalog.md .10x/tickets/2026-07-08-p1-product-ws4d-error-rendering-docs.md .10x/tickets/done/2026-07-08-p1-product-ws4c-error-suggestions.md .10x/evidence/2026-07-08-p1-product-ws4c-error-suggestions.md .10x/reviews/2026-07-08-p1-product-ws4c-error-suggestions-review.md .10x/evidence/2026-07-08-p1-product-ws4-error-catalog-shaping.md .10x/reviews/2026-07-08-p1-product-ws4-error-catalog-shaping-review.md`: passed.
- `gitleaks dir --no-banner --redact <each repaired WS4 record path>` for the parent, WS4D, WS4C ticket, WS4C evidence, WS4C review, shaping evidence, and shaping review: passed; no leaks found.

Parent integration rerun after worker completion:

- Source and record inspection over `crates/cdf-cli/src/output.rs`, `crates/cdf-cli/src/suggestions.rs`, `crates/cdf-cli/src/args.rs`, `crates/cdf-cli/src/context.rs`, `crates/cdf-cli/src/destination_uri.rs`, destination error wrappers, focused tests, this ticket, evidence, and review found no blocking issues.
- Stale-reference and banned-demo-phrase scans over `.10x` and `crates/cdf-cli/src`: no matches.
- `cargo fmt --all -- --check`: pass.
- `git diff --check`: pass.
- `cargo test -p cdf-cli --locked --no-fail-fast`: pass; 181 library tests, 1 integration test, and 0 doctests passed.
- `cargo check -p cdf-cli --all-targets --all-features --locked`: pass.
- `cargo check -p cdf-cli --all-targets --no-default-features --locked`: pass.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: pass.
- `cargo clippy -p cdf-cli --all-targets --all-features --locked -- -D warnings`: pass.
- `cargo clippy -p cdf-cli --all-targets --no-default-features --locked -- -D warnings`: pass.
- `semgrep --config auto crates/cdf-cli/src .10x/evidence/2026-07-08-p1-product-ws4c-error-suggestions.md .10x/reviews/2026-07-08-p1-product-ws4c-error-suggestions-review.md .10x/tickets/done/2026-07-08-p1-product-ws4c-error-suggestions.md`: pass; 48 files scanned, 51 rules run, 0 findings.
- `jscpd . --reporters json,console --output reports/ai-quality/jscpd-ws4c-parent --ignore "**/target/**,**/.git/**,**/reports/**"`: pass; repository-wide 657 clones, 7,507 duplicated lines, 6.140996695134322 percent duplication, and `newClones: 0`.
- `rust-code-analysis-cli -m -p crates/cdf-cli/src -O json -o reports/ai-quality/rust-code-analysis-ws4c-parent`: pass.
- `scc crates/cdf-cli/src .10x/tickets/done/2026-07-08-p1-product-ws4c-error-suggestions.md .10x/evidence/2026-07-08-p1-product-ws4c-error-suggestions.md .10x/reviews/2026-07-08-p1-product-ws4c-error-suggestions-review.md --by-file --format json --output reports/ai-quality/scc-ws4c-parent.json`: pass.
- `gitleaks dir --no-banner --redact crates/cdf-cli/src`: pass; no leaks found.
- `gitleaks dir --no-banner --redact .10x/evidence/2026-07-08-p1-product-ws4c-error-suggestions.md`: pass; no leaks found after this parent-evidence append.
- `gitleaks dir --no-banner --redact .10x/reviews/2026-07-08-p1-product-ws4c-error-suggestions-review.md`: pass; no leaks found.
- `gitleaks dir --no-banner --redact .10x/tickets/done/2026-07-08-p1-product-ws4c-error-suggestions.md`: pass; no leaks found.
- `cargo deny --locked check advisories licenses sources`: pass.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: pass under the existing ratified `paste` advisory exception.
- `cargo vet --locked --no-minimize-exemptions`: pass; 455 exempted.
- `tools/codeql-rust-quality.sh --database target/quality/codeql-db-rust`: pass; reused the fresh Rust database under `target/quality/codeql-db-rust`.
- `jq '[.runs[].results[]?] | length' target/quality/reports/codeql-rust-current.sarif`: `0`.
- Source-only `rg -n "\bunsafe\b" crates/cdf-cli/src`: no matches; exit 1 from `rg` because no matches were found.

CodeQL extraction metrics from the helper:

- Files extracted total: 247.
- Files extracted with errors: 194.
- Files extracted without errors: 53.
- Extraction errors: 0.
- Extraction warnings: 3,751.
- Unresolved macro calls: 5,818.

## What this supports or challenges

This supports WS4C acceptance criteria:

- Unknown command and subcommand errors suggest the nearest valid command when confidence is high enough.
- Unknown resource errors suggest nearest configured project resource ids when inventory exists.
- Destination errors suggest configured environment selectors or expected URI shapes without exposing resolved secret values or userinfo credentials.
- Suggestions are deterministic, bounded, and omitted for low-confidence command matches and no-inventory resource cases.
- JSON exposes additive `error.suggestions`; existing JSON error fields remain present.
- Focused tests cover high-confidence, low-confidence, no-inventory, and redacted-secret cases.

## Limits

The current project model has environment destinations, not a separate named-destination registry, so destination suggestions are limited to `--env <name>` selectors and supported URI shapes. WS4D still owns renderer-integrated human error presentation and generated error documentation.

jscpd continues to report pre-existing repository duplication, but the WS4C run reports `newClones: 0`. CodeQL remains useful but noisy for Rust extraction; the SARIF result count is 0, while extractor warnings and unresolved macro calls remain a known limitation consistent with prior WS4 records.
