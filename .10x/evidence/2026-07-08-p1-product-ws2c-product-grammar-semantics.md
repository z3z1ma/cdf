Status: recorded
Created: 2026-07-08
Updated: 2026-07-13
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws2c-product-grammar-semantics.md, .10x/decisions/superseded/cli-command-grammar-and-parser.md, .10x/specs/project-cli-observability-security.md

# P1 product WS2C product grammar semantics evidence

## What was observed

The WS2C implementation updates the CLI parser and command modules so the ratified product grammar works for `plan`, `explain`, `run`, `state show/history/rewind`, bare `resume`, `replay package`, and `backfill` while preserving existing JSON envelopes and legacy flags.

Changed source files:

- `crates/cdf-cli/src/args.rs`
- `crates/cdf-cli/src/destination_uri.rs`
- `crates/cdf-cli/src/scan_command.rs`
- `crates/cdf-cli/src/run_command.rs`
- `crates/cdf-cli/src/state_command.rs`
- `crates/cdf-cli/src/state_command/recover.rs`
- `crates/cdf-cli/src/resume_command.rs`
- `crates/cdf-cli/src/replay_command.rs`
- `crates/cdf-cli/src/backfill_command.rs`
- `crates/cdf-cli/src/tests.rs`

Observed behavior covered by tests:

- `cdf plan local.events` defaults the destination target from the resource and remains no-write.
- `cdf explain local.events --to duckdb://.cdf/explain.duckdb` resolves the selected destination URI and remains no-write.
- `cdf run local.events --to duckdb://.cdf/short-form.duckdb` succeeds with run-ledger minted run id, CLI-minted package/checkpoint ids, default `cdf-run` pipeline, and derived `events` target.
- Legacy explicit `run` forms continue to pass in the full cdf-cli suite.
- `cdf state show/history local.events --scope kind=resource` reads the default product-run pipeline and preserves legacy `--scope-json` coverage elsewhere in the suite.
- `cdf state rewind local.events --scope kind=resource --to checkpoint-state-product-first` mints a `rewind-marker-*` checkpoint when `--marker-checkpoint` is omitted.
- Bare `cdf resume` returns a no-op report when the run ledger has no interrupted runs, selects exactly one interrupted run, and fails closed with exit 78 when multiple interrupted runs exist.
- `cdf resume <run-id>` remains accepted.
- `cdf replay package <pkg-dir>` defaults to the selected environment destination and replays from artifacts without source contact.
- Postgres replay target/dedup safety remains covered by existing full-suite tests.
- `cdf backfill <resource> --from ... --to ...` no longer requires `--target`; dry planner/runtime rejection coverage remains no-write.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

- `cargo check -p cdf-cli --locked`
- `cargo test -p cdf-cli --locked product -- --nocapture`
- `cargo test -p cdf-cli --locked resume_bare -- --nocapture`
- `cargo test -p cdf-cli --locked replay_package_without_to -- --nocapture`
- `cargo test -p cdf-cli --locked "plan_json_exposes" -- --nocapture`
- `cargo test -p cdf-cli --locked "explain_json_exposes" -- --nocapture`
- `cargo test -p cdf-cli --locked "parser_" -- --nocapture`
- `cargo test -p cdf-cli --locked "backfill_rejects_file_resource_without_runtime_writes" -- --nocapture`
- `cargo test -p cdf-cli --locked "run_missing_resource" -- --nocapture`
- `cargo test -p cdf-cli --locked`
- `cargo fmt --all -- --check`
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`
- `git diff --check -- <touched cdf-cli files>`
- `rg -n '\bunsafe\b|extern "|\*const|\*mut|unsafe impl|impl (Send|Sync)' <touched cdf-cli files>`
- Source-only gitleaks over a temporary copy of touched files, report stored at `.10x/evidence/.storage/2026-07-08-ws2c-gitleaks.json`
- `jscpd --format rust --min-lines 8 --min-tokens 50 --reporters console,json --output .10x/evidence/.storage/2026-07-08-ws2c-jscpd --exit-code 1 crates/cdf-cli/src/args.rs crates/cdf-cli/src/tests.rs`

Final observed results:

- `cargo check -p cdf-cli --locked`: passed.
- Focused parser/product/resume/replay/plan/explain/backfill/run tests: passed.
- `cargo test -p cdf-cli --locked`: passed, 142 lib tests plus 1 integration test plus doc tests.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- `git diff --check -- <touched cdf-cli files>`: passed.
- Direct unsafe scan: no matches; `rg` exited 1 because it found no unsafe tokens.
- Gitleaks: passed; report length is 0 findings at `.10x/evidence/.storage/2026-07-08-ws2c-gitleaks.json`.
- jscpd: completed and reported existing duplication in `args.rs`/`tests.rs`: 59 clones, 715 duplicated lines, 9.18 percent duplicated lines, `newClones: 0`. Raw report is `.10x/evidence/.storage/2026-07-08-ws2c-jscpd/jscpd-report.json`.

Parent verification repeated the closure-relevant checks after the worker report:

- `cargo fmt --all -- --check`: passed.
- `cargo check -p cdf-cli --locked`: passed.
- `cargo test -p cdf-cli --locked`: passed, 142 lib tests plus 1 integration test plus doc tests.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- Scoped `git diff --check` over WS2C source and records: passed.
- Repository forbidden-phrase scan excluding `target/`: no matches.
- Direct unsafe-token scan over the touched Rust files: no matches.
- `rust-code-analysis-cli -m` over the touched CLI Rust files: passed, JSON metrics written under `target/quality/reports/ws2c-rust-code-analysis`.
- Parent jscpd over `args.rs` and `tests.rs`: passed under the ratified duplicate budget, with 59 clones, 715 duplicated lines, and 9.18 percent duplicated lines in `target/quality/reports/ws2c-jscpd-parent/jscpd-report.json`.
- `semgrep scan --config p/rust --error` over the touched files: passed with 0 findings.
- Parent gitleaks over `crates/cdf-cli/src`: passed with no findings.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: passed, with only the already-ratified `paste` advisory ignored.
- `cargo deny --locked check advisories licenses sources`: passed.
- `cargo vet --locked --no-minimize-exemptions`: passed.
- `osv-scanner scan source --lockfile Cargo.lock`: returned the already-ratified `RUSTSEC-2024-0436` only.
- `cargo machete`: passed with no unused dependencies.
- `scc --format json` over the touched Rust files recorded `Rust files=5 code=2633 complexity=68`.
- `tools/codeql-rust-quality.sh`: passed using the reusable database path `target/quality/codeql-db-rust`; the helper refreshed the database because the Rust source fingerprint changed, then produced `target/quality/reports/codeql-rust-current.sarif` with 0 results.

## What this supports or challenges

This supports closing WS2C because the implemented grammar paths have direct focused coverage and the full cdf-cli regression suite remained green. JSON and exit compatibility were preserved through existing tests and new tests that inspect the stable command envelopes for the changed commands.

The jscpd result challenges broad duplication quality for the existing large CLI test file, but not this ticket's acceptance: the report shows no new clones and the remaining clones are broad test-suite scaffolding outside the WS2C behavioral change.

## Limits

This evidence does not cover completions, man pages, help snapshot ownership, renderer migration, error catalog migration, docs, release workflows, or CI because those are explicit WS2C exclusions. Bare `resume` drains only the exactly-one interrupted-run case; multiple interrupted runs fail closed with exit 78 until lower-layer multi-run drain behavior exists. CodeQL still reports extractor macro warnings that are covered by `.10x/knowledge/quality-gate-execution.md`; SARIF security/query results were empty.
