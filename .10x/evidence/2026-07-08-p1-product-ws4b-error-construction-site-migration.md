Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws4b-error-construction-site-migration.md, .10x/specs/cli-error-experience-catalog.md, .10x/specs/project-cli-observability-security.md

# P1 WS4B error construction-site migration evidence

## What was observed

`cdf-cli` error construction sites were migrated from only the WS4A generic foundation toward stable command-family product codes and remediation:

- `crates/cdf-cli/src/error_catalog.rs` now defines command-family mappings for CLI JSON/artifacts, project init/IO, contract arguments/lockfile, scan/preview, destination unsupported, run arguments/artifact paths, replay arguments/contracts, resume ledger/multi-run gaps, state scopes, package artifacts, SQL query/result/internal, status freshness, and doctor drift.
- `crates/cdf-cli/src/output.rs` now exposes `CliError::usage_with`, `CliError::not_supported_with`, and `CliError::mapped` so call sites can supply stable mappings while preserving `ErrorKind`, `exit_code`, and `not_supported` behavior.
- Direct parser grammar `CliError::usage` sites in `crates/cdf-cli/src/args.rs` remain on the documented generic `CDF-CLI-USAGE` mapping. This is intentional because they are pure CLI grammar/parser errors rather than domain-specific runtime failures.
- Direct `CliError::not_supported` construction in production modules was replaced with `not_supported_with` mappings that keep exit 78 and still name the required lower layer.
- Direct `CliError::from(CdfError::...)` construction in production command modules was replaced where the construction site had command-family semantics. Lower-layer `?` and `.map_err(CliError::from)` conversions remain covered by the documented generic lower-layer mapping from WS4A.
- Redaction-sensitive tests cover destination URI/userinfo paths, secret references, Python interpreter stdout/stderr, SQL text, and project/state paths. Existing Python doctor tests are not error-envelope tests because `doctor` reports failed checks in a command-success envelope with nonzero domain status.

## Construction-site inventory

Before migration, source inspection found direct construction clusters in:

- `args.rs`: parser and command grammar usage errors.
- `scan_command.rs`: preview runtime not-supported, scan argument validation, destination planning not-supported.
- `run_command.rs`: `run --loop`, missing run resource, destination not-supported, artifact parent creation.
- `replay_command.rs`: Postgres replay target/merge-dedup arguments, replay target contract mismatch, destination not-supported.
- `resume_command.rs` and `resume_command/destination.rs`: missing run ledger, bare multi-run resume, destination not-supported.
- `backfill_command.rs`: destination not-supported.
- `state_command.rs`: scope parser validation.
- `contract_command.rs`: unknown policy, lockfile write/missing-lock errors.
- `package_command.rs`: archive format and package-directory read errors.
- `cli_artifacts.rs`: artifact freshness and artifact-generation internal errors.
- `project_command.rs`: current-directory and init name errors.
- `system_sql.rs`: read-only SQL grammar, SQLite/query/json conversion errors.
- `status_freshness.rs`: JSON/time/SQLite internal status failures.
- `doctor_drift.rs`: SQLite and JSON drift-check data errors.
- `commands.rs` and `output.rs`: JSON serialization helpers.

After migration, `rg -n "CliError::(usage|not_supported)|CliError::from\(CdfError::" crates/cdf-cli/src` shows:

- `CliError::usage_with`, `CliError::not_supported_with`, and `CliError::mapped` at migrated command-family construction sites.
- Documented generic `CliError::usage` sites in `args.rs`.
- Test-only direct `CliError::not_supported` and `CliError::from(CdfError::...)` coverage for WS4A compatibility behavior.
- No direct production `CliError::from(CdfError::...)` construction sites remain in command modules.

## Tests and checks run

Parent integration rerun after record-reference cleanup:

- Stale-reference and banned-demo-phrase scan over `.10x` and `crates/cdf-cli/src`: no matches after the moved-ticket references and stale WS4B wording were repaired.
- `cargo fmt --all -- --check`: pass.
- `git diff --check`: pass.
- `cargo test -p cdf-cli --locked --no-fail-fast`: pass; 177 library tests, 1 integration test, and 0 doctests passed.
- `cargo check -p cdf-cli --all-targets --all-features --locked`: pass.
- `cargo check -p cdf-cli --all-targets --no-default-features --locked`: pass.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: pass.
- `cargo clippy -p cdf-cli --all-targets --all-features --locked -- -D warnings`: pass.
- `cargo clippy -p cdf-cli --all-targets --no-default-features --locked -- -D warnings`: pass.
- `semgrep --config auto crates/cdf-cli/src .10x/evidence/2026-07-08-p1-product-ws4b-error-construction-site-migration.md .10x/reviews/2026-07-08-p1-product-ws4b-error-construction-site-migration-review.md .10x/tickets/done/2026-07-08-p1-product-ws4b-error-construction-site-migration.md`: pass; 47 files scanned, 51 rules run, 0 findings.
- `jscpd . --reporters json,console --output reports/ai-quality/jscpd-ws4b-parent --ignore "**/target/**,**/.git/**,**/reports/**"`: pass; repository-wide 651 clones, 7,442 duplicated lines, 6.1103% duplicated lines, `newClones: 0`.
- `rust-code-analysis-cli -m -p crates/cdf-cli/src -O json -o reports/ai-quality/rust-code-analysis-ws4b-parent`: pass after creating the output directory required by this tool.
- `scc crates/cdf-cli/src .10x/tickets/done/2026-07-08-p1-product-ws4b-error-construction-site-migration.md .10x/evidence/2026-07-08-p1-product-ws4b-error-construction-site-migration.md .10x/reviews/2026-07-08-p1-product-ws4b-error-construction-site-migration-review.md --by-file --format json --output reports/ai-quality/scc-ws4b-parent.json`: pass.
- `gitleaks dir --no-banner --redact crates/cdf-cli/src`: pass; no leaks found.
- `gitleaks dir --no-banner --redact .10x/evidence/2026-07-08-p1-product-ws4b-error-construction-site-migration.md`: pass; no leaks found.
- `gitleaks dir --no-banner --redact .10x/reviews/2026-07-08-p1-product-ws4b-error-construction-site-migration-review.md`: pass; no leaks found.
- `gitleaks dir --no-banner --redact .10x/tickets/done/2026-07-08-p1-product-ws4b-error-construction-site-migration.md`: pass; no leaks found.
- `cargo deny --locked check advisories licenses sources`: pass.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: pass under the ratified temporary `paste` advisory exception.
- `cargo vet --locked --no-minimize-exemptions`: pass; 455 exempted.
- `tools/codeql-rust-quality.sh --database target/quality/codeql-db-rust`: pass; refreshed the reusable Rust database because the Rust source fingerprint changed.
- `jq '[.runs[].results[]?] | length' target/quality/reports/codeql-rust-current.sarif`: `0`.
- `rg -n "\bunsafe\b" crates/cdf-cli/src .10x/tickets/done/2026-07-08-p1-product-ws4b-error-construction-site-migration.md .10x/evidence/2026-07-08-p1-product-ws4b-error-construction-site-migration.md .10x/reviews/2026-07-08-p1-product-ws4b-error-construction-site-migration-review.md`: no matches; exit 1 from `rg` because no matches were found.

Focused and package tests:

- `cargo test -p cdf-cli migrated_command_family_errors_include_code_and_remediation --locked`: pass.
- `cargo test -p cdf-cli --locked --no-fail-fast`: pass; 177 library tests, 1 integration test, and 0 doctests passed.

Cargo and lint gates:

- `cargo fmt --all`: pass.
- `cargo fmt --all -- --check`: pass.
- `cargo check -p cdf-cli --all-targets --locked`: pass.
- `cargo check -p cdf-cli --all-targets --all-features --locked`: pass.
- `cargo check -p cdf-cli --all-targets --no-default-features --locked`: pass.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: pass.
- `cargo clippy -p cdf-cli --all-targets --all-features --locked -- -D warnings`: pass.
- `cargo clippy -p cdf-cli --all-targets --no-default-features --locked -- -D warnings`: pass.

Repository reports:

- `cargo metadata --format-version=1 --locked > reports/ai-quality/cargo-metadata.json`: pass.
- `cargo tree -p cdf-cli --locked > reports/ai-quality/ws4b-cdf-cli-cargo-tree.txt`: pass.
- `cargo tree -p cdf-cli --locked -d > reports/ai-quality/ws4b-cdf-cli-cargo-tree-duplicates.txt`: pass.
- `jscpd . --reporters json,console --output reports/ai-quality/jscpd-ws4b --ignore "**/target/**,**/.git/**,**/reports/**"`: pass; repository-wide 651 clones, 7,442 duplicated lines, 6.1103% duplicated lines, `newClones: 0`.
- jscpd touched-file query: 133 duplicate records involve touched `cdf-cli` source files, all under `newClones: 0`; this indicates existing duplication patterns rather than new clone introduction.
- `rust-code-analysis-cli -m -p crates/cdf-cli/src -O json -o reports/ai-quality/rust-code-analysis-ws4b`: pass.

Security and supply-chain:

- `semgrep --config auto crates/cdf-cli/src`: pass; 44 files scanned, 51 rules run, 0 findings.
- `cargo deny check`: pass with existing duplicate dependency warnings; final result `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo audit`: pass with 1 allowed warning for unmaintained `paste` (`RUSTSEC-2024-0436`).
- `cargo vet --locked --no-minimize-exemptions`: pass; 455 exempted.
- `gitleaks detect --no-banner --redact --source .`: failed due 2 leaks found in whole repository history. Output did not include redacted file details in the captured summary. This appears pre-existing and unrelated to the touched source.
- `gitleaks dir --no-banner --redact crates/cdf-cli/src`: pass; no leaks found in touched source directory.
- `gitleaks dir --no-banner --redact .10x/evidence/2026-07-08-p1-product-ws4b-error-construction-site-migration.md`: pass; no leaks found.
- `gitleaks dir --no-banner --redact .10x/reviews/2026-07-08-p1-product-ws4b-error-construction-site-migration-review.md`: pass; no leaks found.
- `gitleaks dir --no-banner --redact .10x/tickets/done/2026-07-08-p1-product-ws4b-error-construction-site-migration.md`: pass; no leaks found.
- `tools/codeql-rust-quality.sh --database target/quality/codeql-db-rust`: pass; SARIF result count 0. CodeQL rebuilt the Rust database because Rust source changed. Extractor metrics: 246 Rust files extracted, 192 with errors, 54 without errors, 0 extraction errors, 3,753 extraction warnings, 5,806 unresolved macro calls.

## What this supports or challenges

This supports WS4B acceptance criteria:

- Direct production construction sites now either use a specific stable mapping or are intentionally documented under parser/lower-layer generic mappings.
- Exit codes and `ErrorKind` behavior were preserved by mapping-specific exit codes and `CliError::mapped`.
- Not-supported production paths keep exit 78 and retain the required lower-layer text in the message.
- Tests assert JSON `code` and `remediation` across command families and retain WS4A JSON compatibility.
- Redaction-sensitive coverage remained in place and was extended where error envelopes are produced.

## Limits

Gitleaks whole-history scanning still reports 2 pre-existing leaks outside this scoped change. Scoped scanning over `crates/cdf-cli/src` passed. CodeQL's Rust extractor remains noisy and its 0 SARIF findings are useful but not complete proof of absence of security defects.
