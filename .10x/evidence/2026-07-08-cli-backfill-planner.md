Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-07-cli-backfill-planner.md, .10x/decisions/backfill-window-planner-command-contract.md, .10x/specs/project-cli-observability-security.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/checkpoint-state-commit-gate.md

# CLI backfill planner

## What was observed

`cdf backfill RESOURCE --from CURSOR --to CURSOR --target TARGET [--execute] [--slice-size N]` now has the first ratified bounded cursor-window implementation.

Dry planning stays no-write: it loads project configuration, validates the resource, asks `cdf-project` to plan explicit half-open cursor windows, and reports slices, filters, deterministic package/checkpoint ids, concrete `ScopeKey::Window` scopes, and write effects without creating packages, destination files, checkpoint rows, or run-ledger events.

Execution stays on the general run spine: each planned slice wraps the selected `QueryableResource` in a `WindowScopedResource`, then calls `run_project` with the slice's `EnginePlan`, package id, checkpoint id, destination, and the backfill pipeline id. Executed slices produce ordinary package artifacts, destination receipts, receipt-verified checkpoint commits, and run-ledger events.

Backfill planning is owned by `cdf-project` in `crates/cdf-project/src/backfill.rs`; the CLI owns parsing, destination/resource resolution, error redaction, and JSON/human reporting. Shared run/backfill runtime-source construction now lives in `crates/cdf-cli/src/project_run_resource.rs`, reducing command-level duplication.

## Procedure

Focused behavior checks:

- `cargo test -p cdf-cli backfill`: passed. Covered dry-plan SQL window splitting with no writes, `--resource` alias mismatch before project load, file-resource fail-closed behavior without runtime writes, and a live Postgres SQL source backfilled into DuckDB through `run_project`.
- `cargo test -p cdf-project backfill_planner`: passed. Covered numeric window splitting, concrete window scopes, deterministic id prefixes, no source open during planning, file-incremental rejection, and inverted numeric-bound rejection.
- The live CLI execute test inserted three Postgres rows and ran one `[0, 20)` backfill slice; DuckDB contained two destination rows afterward, the SQLite head existed at `PipelineId("cdf-backfill")` plus `ScopeKey::Window { start: "0", end: "20" }`, and no `ScopeKey::Resource` head was advanced.

Workspace correctness gates:

- `cargo fmt --all`: completed after edits.
- `cargo fmt --all --check`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo test --workspace --locked`: passed, including workspace unit, integration, and doc tests.

Dependency, security, and quality gates:

- `jscpd . --reporters json,console --output reports/ai-quality/jscpd --ignore "**/target/**,**/.git/**,**/reports/**"`: passed. Report summary: 561 clones, 6269 duplicated lines, 5.85% duplicated lines. The run/backfill shared-source refactor removed the new local duplication that initially appeared between CLI command paths.
- `rust-code-analysis-cli -m -p crates -O json -o reports/ai-quality/rust-code-analysis-backfill-current`: passed and wrote 234 per-file JSON metric records.
- `cargo deny check`: passed with the already-ratified duplicate Arrow 58/59 warnings; final gate reported advisories, bans, licenses, and sources OK.
- `cargo audit`: passed with only the already-ratified `RUSTSEC-2024-0436` / `paste 1.0.15` warning.
- `cargo vet`: passed with `Vetting Succeeded (452 exempted)` and the existing stale-exemption prune warning.
- `osv-scanner scan --lockfile Cargo.lock --format json --output reports/ai-quality/osv.json`: reported only `RUSTSEC-2024-0436`, already governed by the native Arrow/DataFusion Parquet policy and DataFusion tuple records.
- `semgrep scan --config p/rust --json --output reports/ai-quality/semgrep-rust.json .`: passed with 0 findings.
- `gitleaks dir --redact --report-format json --report-path reports/ai-quality/gitleaks-crates.json --no-banner --log-level error crates`: passed with no findings.
- `gitleaks dir --redact --report-format json --report-path reports/ai-quality/gitleaks-10x.json --no-banner --log-level error .10x`: passed with no findings.
- `tools/codeql-rust-quality.sh`: passed through the reusable database path `target/quality/codeql-db-rust`. The wrapper refreshed because Rust source content changed, wrote `target/quality/reports/codeql-rust-current.sarif`, and produced 0 SARIF results. Current metrics: Extraction errors 0, Extraction warnings 3566, Files extracted total 234, Macro calls unresolved 5344.

## What this supports

- `cdf backfill` now has a ratified dry-plan default and explicit `--execute` mutation path.
- Dry planning has no package, destination, checkpoint, or run-ledger writes for the covered SQL dry-plan path.
- Executed backfill slices use `run_project` rather than destination/source-specific shortcuts.
- Historical slices commit under concrete window scopes, so they do not overwrite the live resource-scope checkpoint head.
- Unsupported resources fail closed before source contact or runtime writes when they lack cursor-backed queryable semantics.
- Command architecture improved: backfill planning lives below the CLI, and common run/backfill source construction is no longer duplicated in command handlers.

## Limits

The first implementation supports one-slice arbitrary cursor bounds and numeric `--slice-size` splitting. Calendar/date duration splitting remains intentionally excluded by `.10x/decisions/backfill-window-planner-command-contract.md`.

OSV and Cargo Audit still surface the already-ratified `paste` advisory; this evidence found no new advisory. CodeQL completed with 0 actionable SARIF results, but the Rust extractor still reports macro-expansion diagnostics tracked in `.10x/knowledge/quality-gate-execution.md`.
