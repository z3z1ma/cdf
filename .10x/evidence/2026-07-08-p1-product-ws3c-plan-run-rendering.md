Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws3c-plan-run-rendering.md, .10x/decisions/cli-design-language-and-renderer.md, .10x/tickets/done/2026-07-08-p1-product-ws3b-renderer-foundation.md

# P1 product WS3C plan, run, and replay rendering evidence

## What was observed

WS3C migrates `cdf plan`, `cdf explain`, `cdf run`, and `cdf replay package` human output to the renderer introduced by WS3B.

Observed implementation facts:

- `crates/cdf-cli/src/scan_command.rs` now returns `CommandOutput::rendered` for plan/explain and builds a `RenderDocument` with fetch, pushdown, destination, guarantee, contract, migration, migration-table, and next-command blocks.
- Plan/explain JSON report structs and fields are unchanged. The command handoff changed only the human-output path.
- Plan/explain next-command text no longer includes user-minted package or checkpoint identifiers. It renders `cdf run <resource>` when no explicit destination URI is provided and the destination target matches `default_target_for_resource`, adds `--target <target>` only when the target differs, and preserves an explicit plan/explain `--to <destination>` in the next command with URI userinfo redacted for display.
- `crates/cdf-cli/src/reports.rs` now builds renderer documents for run and replay package reports while preserving existing serializable report structs.
- `cdf run` human output now renders run, package, rows, verdicts, receipt, and checkpoint gate panels, including the receipt-verified-before-checkpoint-commit condition where the report has it.
- `cdf replay package` human output now renders replay, destination, duplicate/no-op, receipt, and checkpoint panels through the same renderer primitives.
- `crates/cdf-cli/src/replay_command.rs` and `crates/cdf-cli/src/run_command.rs` now hand off rendered documents and do not change runtime/replay behavior.
- `crates/cdf-cli/src/render/redaction.rs` adds conservative URI userinfo redaction for rendered display strings. It strips credentials before `@` in URI-looking values and leaves non-userinfo values unchanged.
- Focused tests cover rich and headless/static output for plan, run, and replay package; headless/static explain output; explicit `--to` next-command rendering; JSON compatibility for plan, explain, run, and replay package; and adversarial destination URI/userinfo redaction.

## Procedure

Inspected before editing:

- `.10x/tickets/2026-07-08-p1-product-ws3c-plan-run-rendering.md`.
- `.10x/decisions/cli-design-language-and-renderer.md`.
- `.10x/tickets/done/2026-07-08-p1-product-ws3b-renderer-foundation.md`.
- `.10x/evidence/2026-07-08-p1-product-ws3b-renderer-foundation.md`.
- `.10x/reviews/2026-07-08-p1-product-ws3b-renderer-foundation-review.md`.
- `QUALITY.md`.
- `crates/cdf-cli/src/render/**`.
- `crates/cdf-cli/src/scan_command.rs`.
- `crates/cdf-cli/src/reports.rs`.
- `crates/cdf-cli/src/replay_command.rs`.
- `crates/cdf-cli/src/output.rs`.
- Relevant CLI tests in `crates/cdf-cli/src/tests.rs`.

Initial closure verification used a clean detached worktree at `/tmp/firn-ws3c-check` from `main` with only the WS3C source/test diff applied because concurrent WS1C runtime/event-spine work was still dirty then. After WS1C was committed, the explicit-`--to` correction and focused verification were run in the main workspace.

Verification commands run in `/tmp/firn-ws3c-check`:

- `cargo fmt --all -- --check`.
- `cargo check -p cdf-cli --all-targets --locked`.
- `cargo test -p cdf-cli plan_human_ --locked`.
- `cargo test -p cdf-cli explain_human_headless_render_uses_operator_panels --locked`.
- `cargo test -p cdf-cli run_human_ --locked`.
- `cargo test -p cdf-cli replay_package_human_ --locked`.
- `cargo test -p cdf-cli run_rendering_redacts_secret_like_destination_uri_userinfo --locked`.
- `cargo test -p cdf-cli plan_json_exposes_pushdown_ddl_guarantee_and_state_advancement --locked`.
- `cargo test -p cdf-cli explain_json_exposes_destination_plan_without_writes --locked`.
- `cargo test -p cdf-cli run_local_file_to_duckdb_commits_package_rows_mirrors_and_checkpoint --locked`.
- `cargo test -p cdf-cli replay_package_duckdb_replays_from_artifacts_without_source_contact --locked`.
- `cargo test -p cdf-cli --locked`.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`.
- Direct unsafe-token scan over touched WS3C Rust files.
- `git diff --check`.
- `semgrep scan --config p/rust --error <touched WS3C Rust files>`.
- Source-only Gitleaks over a temporary copy of touched WS3C Rust files.
- `jscpd --format rust --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/ws3c-jscpd --exit-code 0 <touched WS3C Rust files>`.
- `scc --by-file <touched WS3C Rust files>`.
- `rust-code-analysis-cli -m -O json` for `redaction.rs`, `scan_command.rs`, and `reports.rs`, with output under `target/quality/reports/ws3c-rust-code-analysis/`.
- `tools/codeql-rust-quality.sh`.
- `jq '[.runs[].results[]?] | length' target/quality/reports/codeql-rust-current.sarif`.

Post-correction verification commands run in the main workspace:

- `cargo fmt --all`.
- `cargo test -p cdf-cli next_run_command --locked`.
- `cargo test -p cdf-cli plan_human_ --locked`.
- `cargo test -p cdf-cli explain_human_headless_render_uses_operator_panels --locked`.
- `cargo check -p cdf-cli --all-targets --locked`.
- `cargo test -p cdf-cli plan_json_exposes_pushdown_ddl_guarantee_and_state_advancement --locked`.
- `cargo test -p cdf-cli explain_json_exposes_destination_plan_without_writes --locked`.
- `cargo test -p cdf-cli run_rendering_redacts_secret_like_destination_uri_userinfo --locked`.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`.
- `cargo fmt --all -- --check`.
- Direct unsafe-token scan over `crates/cdf-cli/src/scan_command.rs` and `crates/cdf-cli/src/tests.rs`.
- `semgrep scan --config p/rust --error crates/cdf-cli/src/scan_command.rs crates/cdf-cli/src/tests.rs`.
- Source-only Gitleaks over a temporary copy of the corrected scan/test files.
- Scoped `git diff --check` over corrected WS3C source and records.
- `cargo test -p cdf-cli --locked` after repairing stale post-WS1C event-count and run-ledger-schema expectations in existing `cdf-cli` tests.
- Parent final scoped quality pass over WS3C source and records: `cargo fmt --all -- --check`, `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`, Semgrep Rust, Gitleaks, `jscpd`, `scc`, `rust-code-analysis-cli`, `tools/codeql-rust-quality.sh`, and `jq '[.runs[].results[]?] | length' target/quality/reports/codeql-rust-current.sarif`.

## Results

- Focused plan rich/headless tests: passed, 2 tests.
- Focused explain headless test: passed, 1 test.
- Focused run rich/headless tests: passed, 2 tests.
- Focused replay rich/headless tests: passed, 2 tests.
- Redaction test: passed, 1 test.
- JSON compatibility sentinels for plan, explain, run, and replay package: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo check -p cdf-cli --all-targets --locked`: passed.
- `cargo test -p cdf-cli --locked`: passed, 157 library tests, 1 integration test, and 0 doctests.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- Direct unsafe-token scan over touched WS3C Rust files: no matches.
- `git diff --check`: passed.
- Semgrep Rust scan: passed with 0 findings.
- Source-only Gitleaks: passed with no leaks.
- `jscpd`: completed with 6 Rust files analyzed, 24 clones, 337 duplicated lines, 3.66% duplicated lines. The clone ranges are in the pre-existing large `crates/cdf-cli/src/tests.rs`; no renderer-local duplicate was identified from the console output.
- `scc`: 6 Rust files analyzed, 9,203 lines, 3,445 code lines, total complexity 88. Per-file complexity highlights: `scan_command.rs` 40, `reports.rs` 16, `replay_command.rs` 16, `run_command.rs` 6, `redaction.rs` 4, `tests.rs` 6.
- `rust-code-analysis-cli`: JSON metric reports captured for focused renderer/report files.
- CodeQL: `tools/codeql-rust-quality.sh` completed and `jq '[.runs[].results[]?] | length' target/quality/reports/codeql-rust-current.sarif` returned `0`. CodeQL emitted the known Rust extractor macro-warning profile during database creation/analysis.

Post-correction main-workspace results:

- Explicit destination correction tests: `cargo test -p cdf-cli next_run_command --locked` passed 2 tests, including URI userinfo redaction and no minted IDs.
- Plan human tests: `cargo test -p cdf-cli plan_human_ --locked` passed 3 tests, including explicit destination plus non-default target preservation.
- Explain human test: passed with explicit `--to duckdb://.cdf/explain-render.duckdb` preserved in the next command.
- Main workspace compile: `cargo check -p cdf-cli --all-targets --locked` passed.
- JSON compatibility sentinels for plan and explain: passed.
- Redaction sentinel: passed.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- Direct unsafe-token scan over corrected WS3C files: no matches.
- Semgrep Rust scan over corrected WS3C files: passed with 0 findings.
- Source-only Gitleaks over corrected WS3C files: passed with no leaks.
- Scoped `git diff --check`: passed.
- `cargo test -p cdf-cli --locked` passed in the main workspace after updating the stale post-WS1C expectations for 13 run events, terminal event index 12, and run-ledger schema version 3: 160 library tests, 1 integration test, and 0 doctests.
- Parent final `cargo fmt --all -- --check`: passed.
- Parent final `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- Parent final Semgrep Rust scan over the six touched Rust files: passed with 0 findings; report path `target/quality/reports/semgrep-p1-ws3c-final.json`.
- Parent final Gitleaks scan over copied touched source and WS3C records: passed with no leaks; report path `target/quality/reports/gitleaks-p1-ws3c-final.json`.
- Parent final `jscpd` over touched source and WS3C records: completed with 24 clones, 337 duplicated Rust lines, 3.63%; clone ranges remain in the pre-existing large `crates/cdf-cli/src/tests.rs` test surface rather than renderer-local source. Report path `target/quality/reports/jscpd-p1-ws3c-final/jscpd-report.json`.
- Parent final `scc` metrics were written to `target/quality/reports/scc-p1-ws3c-final.json`.
- Parent final `rust-code-analysis-cli` metrics were written under `target/quality/reports/rust-code-analysis-p1-ws3c-final/`.
- Parent final CodeQL used `target/quality/codeql-db-rust`, completed analysis, and `jq '[.runs[].results[]?] | length' target/quality/reports/codeql-rust-current.sarif` returned `0`. The known Rust extractor warning profile remained: macro-resolution warnings and partial extraction coverage, with 242 Rust files scanned.

## What this supports

This supports closing WS3C. The migrated command families use the renderer panel language for human output, JSON behavior remains compatible, plan/explain next commands avoid user-minting system identifiers and preserve explicit destinations, redaction covers secret-like URI userinfo in rendered output, and focused WS3C verification plus main-workspace compile/clippy/full `cdf-cli` tests passed.

## Limits

This evidence does not claim migration of recovery/state/backfill/inspect-run command families; WS3D owns those. It does not implement live progress; WS5 owns progress. It does not implement the migration gate for all future raw human output; WS3E owns that.
