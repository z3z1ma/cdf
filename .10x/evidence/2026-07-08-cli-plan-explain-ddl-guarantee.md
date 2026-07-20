Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-07-cli-plan-explain-ddl-guarantee.md, .10x/specs/project-cli-observability-security.md, .10x/specs/destination-receipts-guarantees.md, .10x/specs/run-orchestration-ledger.md

# CLI plan/explain DDL and guarantee output

## What was observed

`cdf plan` and `cdf explain` now require `--target` and return a no-write destination planning report for the selected environment destination. The JSON report includes resource schema fields and schema hash, destination identity and sheet, DDL/migration preview, synthetic planning package inputs, derived delivery guarantee details, pushdown fidelity, and the state-advancement rule.

The implementation adds a lower `cdf-project` planning facade over `ResolvedProjectDestination::plan_resource_commit`. It creates synthetic commit/state inputs and asks each destination runtime to dry-plan a resource commit without creating packages, checkpoints, destination roots, DuckDB files, or receipts. DuckDB plans from the resource schema without creating a database; filesystem Parquet dry-plans without materializing the root; Postgres plans from resource schema and synthetic replay inputs.

Unsupported destination/disposition combinations fail closed during destination planning. A Parquet `merge` plan returns an error and does not claim a delivery guarantee.

## Procedure

Focused behavior checks:

- `cargo test -p cdf-cli plan_ --locked`: passed, including plan JSON, merge guarantee, and unsupported disposition coverage.
- `cargo test -p cdf-cli explain_json_exposes_destination_plan_without_writes --locked`: passed.
- `cargo test -p cdf-project destination_planning_facade_ --locked`: passed.

Workspace correctness gates:

- `cargo fmt --all`: completed after edits.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo clippy -p cdf-cli -p cdf-project -p cdf-engine --all-targets --locked -- -D warnings`: passed.
- `cargo clippy -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres --all-targets --locked -- -D warnings`: passed.
- `cargo test --workspace --all-targets --locked`: passed.

Dependency, security, and quality gates:

- `cargo machete --with-metadata`: passed; no unused dependencies reported.
- `jscpd crates/cdf-cli/src crates/cdf-project/src/runtime crates/cdf-dest-duckdb/src crates/cdf-dest-parquet/src --reporters json,console --output reports/ai-quality/jscpd-cli-plan-explain --ignore "**/target/**,**/.git/**,**/reports/**"`: passed. Final report: 127 clones, 1259 duplicated lines, 9329 duplicated tokens, 5.90% duplicated lines, 6.99% duplicated tokens, and `newClones = 0`.
- `rust-code-analysis-cli -m` completed for `crates/cdf-cli/src`, `crates/cdf-project/src/runtime`, `crates/cdf-dest-duckdb/src`, `crates/cdf-dest-parquet/src`, and `crates/cdf-dest-postgres/src`, with JSON metric artifacts under `reports/ai-quality/rust-code-analysis-cli-plan-explain-*`.
- `cargo deny check`: passed with existing duplicate-version warnings and final `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo audit --json > reports/ai-quality/cargo-audit-cli-plan-explain.json`: passed.
- `cargo vet`: passed with `Vetting Succeeded (452 exempted)` and the existing prune warning for unrelated stale exemptions.
- `semgrep scan --config p/rust --error --json --output reports/ai-quality/semgrep-rust-cli-plan-explain.json .`: passed with 0 findings.
- `osv-scanner scan --lockfile Cargo.lock --format json --output reports/ai-quality/osv-cli-plan-explain.json`: reported only the already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` advisory governed by `.10x/decisions/superseded/datafusion-git-pin-arrow59-tuple.md` and `.10x/evidence/2026-07-08-cargo-vet-datafusion-git-policy.md`.
- `tools/codeql-rust-quality.sh`: passed using reusable DB path `target/quality/codeql-db-rust`. It refreshed because Rust source content changed, then wrote `target/quality/reports/codeql-rust-current.sarif`; non-diagnostic/security result count was 0. Current extractor metrics still show the known Rust macro-expansion limitation.
- Source-only `gitleaks detect --no-git --redact` over `crates/cdf-cli/src`, `crates/cdf-project/src/runtime`, `crates/cdf-dest-duckdb/src`, and `crates/cdf-dest-parquet/src`: passed with no leaks found.
- `gitleaks protect --staged --redact --report-format json --report-path reports/ai-quality/gitleaks-cli-plan-explain-staged.json --no-banner`: passed with no leaks found.
- Full-history `gitleaks detect --source . --redact --report-format json --report-path reports/ai-quality/gitleaks-cli-plan-explain.json` reported two historical `generic-api-key` findings in removed `src/cdf/...` paths. The historical triage owner is `.10x/tickets/done/2026-07-08-historical-gitleaks-findings-triage.md`.

## What this supports

- `cdf plan` and `cdf explain` expose scan/resource schema, destination DDL preview, destination sheet details, delivery guarantee, pushdown fidelity, and state advancement before bytes move.
- Guarantee strings are mechanically checked against destination sheet/idempotency/disposition facts and the destination commit plan.
- Plan/explain are no-write commands for the covered DuckDB and filesystem Parquet paths.
- Unsupported destination/disposition combinations fail closed without a synthetic guarantee.
- The CLI-specific destination resolution helper reduces scan/resume duplication rather than adding another destination-resolution branch.

## Limits

The source-only Gitleaks scans support this change; the separate full-history findings remain repository-level history work. CodeQL completed with zero actionable SARIF results, but the Rust extractor still reports macro-expansion diagnostics already tracked as a tool limitation.
