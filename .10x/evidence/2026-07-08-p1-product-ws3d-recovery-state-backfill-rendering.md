Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws3d-recovery-state-backfill-rendering.md, .10x/decisions/cli-design-language-and-renderer.md, .10x/tickets/done/2026-07-08-p1-product-ws3b-renderer-foundation.md, .10x/tickets/done/2026-07-08-p1-product-ws3c-plan-run-rendering.md

# P1 product WS3D recovery, state, and backfill rendering evidence

## What was observed

WS3D migrates `resume`, `state show/history/rewind/migrate/recover`, `backfill`, and `inspect run` human output to the renderer introduced by WS3B.

Observed implementation facts:

- `crates/cdf-cli/src/backfill_command.rs` now returns rendered human output for dry plans and executed backfills while preserving the existing `BackfillCliReport` JSON structure. Human output shows backfill bounds, target, pipeline, write effects, slice table, mutation status, and next command.
- `crates/cdf-cli/src/inspect_run_command.rs` now renders recovery, artifacts, pointers, duplicate status, package artifact availability, and event tables. Missing package and receipt artifacts are explicit, and displayed package paths pass through renderer URI userinfo redaction.
- `crates/cdf-cli/src/resume_command.rs` and `crates/cdf-cli/src/resume_command/report.rs` now render bare no-op, successful, and fail-closed resume reports through panels. The main recovery panel shows failed phase, durable artifacts, mutation required/performed, guidance, run-ledger event counts, and next command while preserving nonzero fail-closed exit codes.
- `crates/cdf-cli/src/state_command.rs`, `state_command/migrate.rs`, and `state_command/recover.rs` now render state show/history/rewind/migrate/recover output with panels and tables. Rewind shows marker and packages-ahead mutation state. Recover shows receipt/checkpoint facts, destination-row non-write behavior, and evidence limits.
- Parent review added a state follow-up renderer correction: `--scope-json` object input is normalized to taught `--scope key=value` follow-up commands when lossless, with `--scope-json` retained only for scopes that cannot be expressed as string key/value pairs.
- JSON output paths still pass the same report values through `serde_json` envelopes; no parser grammar, runtime mutation behavior, or JSON fields were intentionally changed.
- Focused tests in `crates/cdf-cli/src/tests.rs` cover headless and forced-rich rendering for the WS3D command families, JSON compatibility sentinels, missing-artifact display, and adversarial rendered redaction of URI userinfo in inspect/resume output.

## Procedure

Inspected before editing:

- `.10x/tickets/done/2026-07-08-p1-product-ws3d-recovery-state-backfill-rendering.md`.
- `.10x/decisions/cli-design-language-and-renderer.md`.
- `.10x/tickets/done/2026-07-08-p1-product-ws3b-renderer-foundation.md`.
- `.10x/tickets/done/2026-07-08-p1-product-ws3c-plan-run-rendering.md`.
- `.10x/evidence/2026-07-08-p1-product-ws3b-renderer-foundation.md`.
- `.10x/reviews/2026-07-08-p1-product-ws3b-renderer-foundation-review.md`.
- `.10x/evidence/2026-07-08-p1-product-ws3c-plan-run-rendering.md`.
- `.10x/reviews/2026-07-08-p1-product-ws3c-plan-run-rendering-review.md`.
- `QUALITY.md`.
- Relevant renderer, output, backfill, inspect-run, resume, state, and `cdf-cli` test modules.

Verification commands run in the main workspace:

- `cargo check -p cdf-cli --all-targets --locked`.
- `cargo test -p cdf-cli backfill --locked`.
- `cargo test -p cdf-cli inspect_run --locked`.
- `cargo test -p cdf-cli resume --locked`.
- `cargo test -p cdf-cli state_ --locked`.
- `cargo fmt --all -- --check`.
- `cargo test -p cdf-cli --locked`.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`.
- Parent rerun: `cargo test -p cdf-cli state_followup_commands_render_scope_pairs_for_scope_json_objects --locked`.
- Parent rerun: `cargo test -p cdf-cli --locked`.
- Parent rerun: `cargo fmt --all -- --check`.
- Parent rerun: `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`.
- Parent rerun: `git diff --check` over scoped WS3D records and source files.
- Parent rerun: forbidden legacy-demo-phrase scan across `.10x`, docs, crates, root docs, changelog, and tools.
- Direct unsafe-token scan over touched WS3D Rust files.
- Parent rerun: direct unsafe/extern scan over touched WS3D Rust files.
- `semgrep scan --config p/rust --error` over touched WS3D Rust files.
- `jscpd --format rust --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/ws3d-jscpd --exit-code 0` over touched WS3D Rust files.
- `scc --by-file --format json --output target/quality/reports/ws3d-scc.json` over touched WS3D Rust files.
- `rust-code-analysis-cli -m -O json` for touched WS3D source files, with reports under `target/quality/reports/ws3d-rust-code-analysis/`.
- Source-plus-record Gitleaks over a temporary copy of touched WS3D source and completed WS3D records, with final report path `target/quality/reports/ws3d-gitleaks-final.json`.
- `tools/codeql-rust-quality.sh`.
- `jq '[.runs[].results[]?] | length' target/quality/reports/codeql-rust-current.sarif`.

## Results

- Focused backfill tests: passed 5 tests, including dry-plan headless/rich rendering and execute-path behavior.
- Focused inspect-run tests: passed 8 tests, including headless/rich rendering, missing artifact display, JSON compatibility, duplicate status, and rendered URI-userinfo redaction.
- Focused resume tests: passed 13 tests, including fail-closed, no-op, replay/recovery, stale-status repair, headless/rich rendering, and rendered URI-userinfo redaction.
- Focused state tests: passed 13 matched tests, including show/history/rewind/migrate/recover rendering and existing state JSON/recovery behavior.
- Parent focused state follow-up test: passed.
- `cargo fmt --all -- --check`: passed.
- Parent `cargo test -p cdf-cli --locked`: passed, 169 library tests, 1 integration test, and 0 doctests.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- Direct unsafe/extern scan over touched WS3D Rust files: no matches. A broader parent token scan matched only `std::ffi::OsString` in tests and did not indicate unsafe boundary work.
- Scoped `git diff --check`: passed.
- Forbidden legacy-demo-phrase scan: no matches.
- Semgrep Rust scan over 8 touched Rust files: passed with 0 findings.
- `jscpd`: completed with 8 Rust files analyzed, 25 clones, 346 duplicated lines, 3.45% duplicated lines. The clone profile is concentrated in the large existing test surface plus repeated CLI fixture shapes; no blocking duplication finding was produced.
- `scc`: 8 Rust files analyzed, 10,041 lines, 3,791 code lines, complexity 169. Report path: `target/quality/reports/ws3d-scc.json`.
- `rust-code-analysis-cli`: reports captured under `target/quality/reports/ws3d-rust-code-analysis/` using current `--paths` syntax.
- Source-plus-record Gitleaks over scoped `target/quality/ws3d-gitleaks-scope`: passed with no leaks.
- CodeQL used `target/quality/codeql-db-rust`, completed analysis, and the SARIF result count was `0`. The known Rust extractor macro-warning profile remained, with 243 Rust files scanned and many macro-resolution warnings.

## What this supports

This supports closing WS3D. The specified recovery-heavy command families now use renderer panels, tables, next-command affordances, and display redaction for human output; JSON output remains stable; focused and full `cdf-cli` verification passed; and the required quality gates completed through CodeQL.

## Limits

The workspace contained unrelated dirty files and records outside the WS3D write scope during verification, including WASM records and other product-lane files. They were not intentionally edited for WS3D and are not claimed by this evidence. This evidence does not claim migration of remaining command families or the future raw-output migration gate.
