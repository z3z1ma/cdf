Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws3b-renderer-foundation.md, .10x/decisions/cli-design-language-and-renderer.md, .10x/specs/project-cli-observability-security.md, .10x/specs/cli-live-progress.md

# P1 product WS3B renderer foundation evidence

## What was observed

WS3B introduced a dedicated `cdf-cli` renderer boundary without migrating command families.

Observed implementation facts:

- `crates/cdf-cli/src/render/**` now exists as non-monolithic modules:
  `config.rs`, `humanize.rs`, `mod.rs`, `primitives.rs`, `redaction.rs`, and `style.rs`.
- `RenderConfig` is the single renderer configuration boundary for display mode, width, `NO_COLOR`, `CLICOLOR_FORCE`, and the parsed `--no-color` flag.
- `RenderConfig::detect` derives TTY/headless from stdout terminal detection, width from `COLUMNS` with a bounded fallback, `NO_COLOR` from env presence, and `CLICOLOR_FORCE` from nonzero env value.
- Headless rendering emits ASCII glyphs, ASCII table/rule borders, and no ANSI even when `CLICOLOR_FORCE` is present.
- `--no-color` is parsed as a global display flag in the existing compatibility pre-pass and is stored on `Cli` without changing command-family semantics.
- Renderer primitives exist for status lines, key-value panels, tables, section rules, humanized rows/bytes/rates/durations, next-command affordances, and exact-value redaction helpers.
- `CommandOutput` now carries `HumanOutput::Plain` or `HumanOutput::Rendered(RenderDocument)`. Existing command families continue to produce `Plain` through the existing `commands::output` and `commands::report_output` helpers.
- `InvocationResult::from_output` renders `HumanOutput` only when `json_mode` is false. The renderer JSON-bypass test proves rendered human text is absent from the JSON success envelope.
- No `crates/cdf-cli/Cargo.toml` or `Cargo.lock` changes were needed; no new renderer dependency was added.
- No plan/run/replay/resume/backfill/state command-family output migration, live progress implementation, event-spine change, or command grammar change beyond `--no-color` plumbing was made.

Foundational snapshot-style tests added:

- rich TTY snapshot covering rule, success status, key-value panel, table, redaction marker, humanized units, and next command;
- headless/static ASCII snapshot covering the same representative primitives without ANSI;
- no-color policy test proving ANSI is disabled while rich glyphs remain available in TTY mode;
- JSON bypass test proving a rendered human document is not evaluated into JSON output;
- width/truncation test proving rule width and table cell ellipsis behavior;
- exact redaction helper test.

## Procedure

Inspected before editing:

- `.10x/tickets/done/2026-07-08-p1-product-ws3b-renderer-foundation.md`.
- `.10x/decisions/cli-design-language-and-renderer.md`.
- `.10x/specs/project-cli-observability-security.md`.
- `.10x/specs/cli-live-progress.md`.
- `QUALITY.md`.
- `crates/cdf-cli/src/output.rs`.
- `crates/cdf-cli/src/lib.rs`.
- `crates/cdf-cli/src/args.rs`.
- Current `cdf-cli` tests in `crates/cdf-cli/src/tests.rs` and `crates/cdf-cli/tests/doctor_env.rs`.

Verification commands run:

- `cargo fmt --all`
- `cargo fmt --all -- --check`
- `cargo test -p cdf-cli render --locked`
- `cargo test -p cdf-cli parser_accepts_no_color_anywhere_without_changing_json_envelope --locked`
- `cargo test -p cdf-cli --locked`
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`
- Direct unsafe scan over touched cdf-cli Rust files:
  `rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|Unpin|Send|Sync" crates/cdf-cli/src/args.rs crates/cdf-cli/src/commands.rs crates/cdf-cli/src/lib.rs crates/cdf-cli/src/output.rs crates/cdf-cli/src/tests.rs crates/cdf-cli/src/render`
- `semgrep scan --config p/rust --error crates/cdf-cli/src/args.rs crates/cdf-cli/src/commands.rs crates/cdf-cli/src/lib.rs crates/cdf-cli/src/output.rs crates/cdf-cli/src/tests.rs crates/cdf-cli/src/render`
- Source-only Gitleaks over a temporary copy of touched source and WS3B records:
  `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/ws3b-gitleaks-final.json /tmp/firn-ws3b-gitleaks-final`
- `jscpd --format rust,markdown --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/ws3b-jscpd-final --exit-code 0 <touched cdf-cli files and WS3B records>`
- `scc --by-file crates/cdf-cli/src/args.rs crates/cdf-cli/src/commands.rs crates/cdf-cli/src/lib.rs crates/cdf-cli/src/output.rs crates/cdf-cli/src/tests.rs crates/cdf-cli/src/render`
- `rust-code-analysis-cli -m -O json -p ...` for representative renderer/output files, with reports captured under `target/quality/reports/ws3b-rust-code-analysis/`.
- `tools/codeql-rust-quality.sh`
- `jq '[.runs[].results[]?] | length' target/quality/reports/codeql-rust-current.sarif`
- `git diff --check`

## Results

- Focused renderer tests: passed, 7 matched tests.
- `--no-color` parser/output sentinel: passed, 1 test.
- `cargo test -p cdf-cli --locked`: passed, 149 library tests, 1 integration test, and 0 doctests.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- Direct unsafe scan: no matches.
- Semgrep Rust scan: passed with 0 findings.
- Source-only Gitleaks: passed with no leaks.
- `git diff --check`: passed.
- `scc`: 11 Rust files analyzed, 8,987 lines, 3,349 code lines, total complexity 126. Renderer-specific complexity highlights: `primitives.rs` complexity 28, `humanize.rs` 19, `config.rs` 8, `style.rs` 5, `redaction.rs` 2.
- `rust-code-analysis-cli`: JSON metrics captured for `primitives.rs`, `config.rs`, `humanize.rs`, and `output.rs` under `target/quality/reports/ws3b-rust-code-analysis/`.
- `jscpd`: completed with 11 Rust files analyzed, 24 clones, 337 duplicated lines, 3.75% duplicated lines. The clone ranges are in the pre-existing large `crates/cdf-cli/src/tests.rs` regions; the new renderer modules did not produce a renderer-local duplicate finding in the console output.
- CodeQL: `tools/codeql-rust-quality.sh` completed successfully using `target/quality/codeql-db-rust`; `jq '[.runs[].results[]?] | length' target/quality/reports/codeql-rust-current.sarif` returned `0`. CodeQL emitted the known macro-heavy Rust extractor warning profile during database creation.
- Supply-chain gates were not rerun because WS3B did not change `Cargo.toml` or `Cargo.lock`.

Parent verification repeated the closure-relevant renderer checks:

- `cargo fmt --all -- --check`: passed.
- `cargo test -p cdf-cli render --locked && cargo test -p cdf-cli parser_accepts_no_color_anywhere_without_changing_json_envelope --locked`: passed, covering 7 renderer-matched tests and the `--no-color` parser sentinel.
- `cargo test -p cdf-cli --locked`: passed, 149 unit tests plus 1 integration test plus doc tests.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- Scoped `git diff --check` over WS3B source and records: passed.
- Direct unsafe-token scan over touched `cdf-cli` files and `render/**`: no matches.
- Repository forbidden demo-phrase scan excluding `target/`: no matches.
- `target/quality/reports/codeql-rust-current.sarif` contained 0 results at parent review time.

## What this supports

This supports closing WS3B. The renderer foundation exists, has a single configuration boundary for required display inputs, exposes the required primitives, keeps ASCII fallback testable, bypasses human rendering in JSON mode, and has rich/headless snapshot-style coverage for representative primitives.

## Limits

This evidence does not claim command-family migration to the renderer. Existing commands still use plain human strings until WS3C/WS3D. This evidence does not implement live progress; WS5 owns that. This evidence does not prove future migration enforcement that rejects new raw human output; WS3E owns that gate.

Unrelated dirty worktree changes existed or appeared during this run in WASM records, WS1B records, and `crates/cdf-project/**`. They were not modified intentionally as part of WS3B and are not claimed by this evidence.
