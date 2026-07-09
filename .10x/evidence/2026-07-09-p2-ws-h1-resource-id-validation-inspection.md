Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-h1-resource-id-validation-inspection.md, .10x/specs/data-onramp-source-experience-cli.md, .10x/decisions/data-onramp-source-identity-preview-disposition.md

# P2 WS-H1 Resource Id Validation And Inspection Evidence

## What Was Observed

The implementation makes declarative source/resource names available on `CompiledResource`, compiles project declarative resource entries with source-file and mapping metadata, validates `[resources."pattern"]` keys against compiled resource ids, and renders the compiled id/source/resource/source-file/mapping status through `cdf inspect resources`.

The canonical default id path is covered by a focused declarative compiler test using `[source.tlc]` plus `[resource.yellow]`, which compiles to `tlc.yellow`.

Explicit `id` compatibility remains preserved because the full `cdf-project` runtime tests already depend on `id = "postgres.orders"` for SQL runtime coverage. The stricter rejection was tried first and failed those existing tests, so the final behavior preserves explicit ids while making the default/new-resource path canonical.

## Procedure

Focused tests run:

- `cargo test -p cdf-declarative source_and_resource_names_form_canonical_compiled_id --locked`: passed.
- `cargo test -p cdf-declarative explicit_resource_id_is_preserved_for_existing_compatibility --locked`: passed.
- `cargo test -p cdf-project declarative_resource_mapping_pattern_must_match_compiled_id --locked`: passed.
- `cargo test -p cdf-project declarative_sql_secret_is_collected_for_validation --locked`: passed after updating the stale SQL fixture mapping from `github.*` to `warehouse.*`.
- `cargo test -p cdf-project general_project_run_rejects_sql_missing_secret_provider_before_writes --locked`: passed.
- `cargo test -p cdf-project general_project_run_executes_table_backed_postgres_sql_resource_stream --locked`: passed.
- `cargo test -p cdf-cli resource_mapping_pattern_mismatch_reports_validate_and_plan_commands --locked`: passed.
- `cargo test -p cdf-cli inspect_human_outputs_use_renderer_for_project_inventory --locked`: passed.

Full crate tests run:

- `cargo test -p cdf-declarative --locked`: passed, 65 unit tests and doc tests.
- Initial `cargo test -p cdf-project --locked`: failed 4 tests. Three failures proved current explicit-id compatibility for `postgres.orders`; one failure found a stale test mapping after swapping a warehouse SQL resource into `BOOK_PROJECT`.
- Final `cargo test -p cdf-project --locked`: passed, 89 unit tests and doc tests.
- `cargo test -p cdf-cli --locked`: passed, 199 library tests, 1 integration test, and doc tests.

Quality gates run:

- `cargo clippy -p cdf-declarative -p cdf-project -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `jscpd --format rust --reporters console --no-colors --no-tips` on the touched Rust files including tests: completed and reported broad pre-existing duplication in large test files.
- `jscpd --format rust --reporters console --no-colors --no-tips` on non-test touched implementation files: completed with 1 small existing clone in `crates/cdf-cli/src/context.rs` and 0.25% duplicated lines.
- `rust-code-analysis-cli -m -O json` on touched implementation files: passed, producing verbose JSON metrics.
- `gitleaks detect --no-git --redact --source <record> --verbose` on the evidence record, review record, closed child ticket, WS-H parent ticket, and P2 parent ticket: passed with no leaks found.
- Banned-phrase/rename scans on touched records found only pre-existing or intentionally referenced text; changed parent-record lines and new evidence/review content introduced no rename cleanup markers or banned placeholders.
- Parent review tightened two scope details before commit: non-mapping project-load errors now keep the existing generic CLI conversion instead of receiving the H1 command prefix, and project resource mapping patterns use only the existing `*` wildcard behavior rather than adding unratified `?` wildcard semantics.

Parent integration verification run before commit:

- Re-ran the focused tests above and the full touched-crate test suites:
  - `cargo test -p cdf-declarative --locked`: passed, 65 unit tests and doc tests.
  - `cargo test -p cdf-project --locked`: passed, 89 unit tests and doc tests.
  - `cargo test -p cdf-cli --locked`: passed, 199 library tests, 1 integration test, and doc tests.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo clippy -p cdf-declarative -p cdf-project -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-p2-h1.json .`: passed with 0 findings on tracked files.
- Source-only Gitleaks snapshot from tracked files plus the new H1 records: `gitleaks detect --no-git --redact --source target/quality/gitleaks-src-p2-h1 --report-format json --report-path target/quality/reports/gitleaks-src-p2-h1.json --verbose`: passed with no leaks found.
- `cargo deny --locked check advisories licenses sources`: passed.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: passed.
- `cargo vet --locked --no-minimize-exemptions`: passed, 455 exempted.
- `osv-scanner scan source --lockfile Cargo.lock --format json > target/quality/reports/osv-p2-h1.json`: exited nonzero only for the already-ratified `paste` advisory `RUSTSEC-2024-0436`.
- `tools/codeql-rust-quality.sh 2>&1 | tee target/quality/reports/codeql-rust-p2-h1.log`: completed through reusable database path `target/quality/codeql-db-rust`. The database refreshed because Rust source content changed. The SARIF contains three `rust/hard-coded-cryptographic-value` findings in pre-existing `crates/cdf-cli/src/tests.rs` backfill fixture literals, already owned by `.10x/tickets/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`; no H1-owned implementation path is implicated.
- Implementation-only `jscpd --format rust --reporters console --no-colors --no-tips` over touched implementation files completed with one small existing clone in `crates/cdf-cli/src/context.rs`, 11 duplicated lines / 0.25%.
- Broader touched-file `jscpd` including tests completed and reported existing large-test duplication, 1,522 duplicated lines / 9.33%.
- `rust-code-analysis-cli -m -O json -p <file>` over touched implementation files wrote `target/quality/reports/rust-code-analysis-p2-h1.jsonl`.
- A banned-phrase, old-name, and placeholder scan over H1 touched records/source returned no matches.
- `rg -n "unsafe"` over H1 touched implementation files returned no matches.

## What This Supports

The tests support that:

- New declarative resources default to compiled ids of `<source>.<resource>`.
- A project mapping pattern that matches zero compiled resource ids fails before `cdf validate` and `cdf plan` proceed.
- The zero-match diagnostic names the unmatched pattern, the compiled id that did exist, and the `[resources."tlc.yellow"]` fix shape.
- The `validate` and `plan` diagnostics name the command that is running and do not mislabel the plan error as `cdf validate`.
- `cdf inspect resources` renders the compiled id, source name, resource name, source file, and mapping status.
- Existing explicit-id SQL runtime compatibility is preserved.

## Limits

The checks were local and did not exercise live TLC public data, `cdf add`, ad-hoc mode, or docs quickstarts because this ticket explicitly excludes those surfaces.

The `jscpd` broad touched-file scan reports existing duplication in large test files. No code change was made for that output because it is unrelated to the resource-id validation and inspection behavior.

The CodeQL SARIF currently contains three pre-existing P1 backfill-test fake-secret fixture findings. They remain outside this P2 H1 ticket and are already owned by `.10x/tickets/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`; this evidence therefore does not claim current-tree CodeQL has zero findings.
