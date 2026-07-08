Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-e3-merge-dedup-live-path.md, .10x/tickets/2026-07-08-p1-contract-depth-program.md

# P1 E3 merge dedup live-path evidence

## What was observed

The E3 slice implements deterministic pre-merge dedup for compiled contract `Dedup { keys, keep }` rules.

- `cdf-contract` evaluates at most one package-order dedup rule over accepted batches, supports `keep = first`, `keep = last`, and `keep = fail`, rejects NULL keys fail-closed, and emits a deterministic `DedupSummary`.
- `cdf-engine` applies dedup only when `EnginePlan.write_disposition == Merge` and the compiled validation program contains a dedup rule. Dedup runs after row verdict/quarantine filtering and before package segment writing and destination mutation.
- `cdf-package` records `stats/dedup-summary.json` as package identity evidence and exposes it through `PackageReader::read_dedup_summary_json`.
- `cdf-project` has live DuckDB merge coverage proving recorded deduped package segments, replay identity, and duplicate redrive behavior.
- `EnginePlan` deserialization now defaults legacy missing `write_disposition` to `Append`, preserving pre-E3 conformance fixtures and preventing a package-fixture compatibility regression.

## Procedure

Focused behavior checks:

- `cargo test --locked -p cdf-contract package_order_dedup -- --nocapture`: 3 passed.
- `cargo test --locked -p cdf-package dedup_summary_round_trips_as_json_identity_evidence -- --nocapture`: 1 passed.
- `cargo test --locked -p cdf-engine dedup -- --nocapture`: 5 passed.
- `cargo test --locked -p cdf-engine engine_plan_deserialization_defaults_legacy_missing_write_disposition_to_append -- --nocapture`: 1 passed.
- `cargo test --locked -p cdf-project merge_dedup_live_run_records_deduped_package_replay_identity_and_duplicate_redrive -- --nocapture`: 1 passed.
- `cargo nextest run -p cdf-contract -p cdf-package -p cdf-engine -p cdf-project --locked -E 'test(package_order_dedup) | test(dedup_summary_round_trips_as_json_identity_evidence) | test(merge_dedup)'`: 8 passed, 132 skipped.

Conformance and collateral checks:

- Initial broad `cargo nextest run -p cdf-contract -p cdf-package -p cdf-engine -p cdf-project -p cdf-conformance --locked` failed in conformance live-run fixtures because serialized legacy `EnginePlan` JSON lacked the new `write_disposition` field.
- The repair added a serde default of `Append` plus `engine_plan_deserialization_defaults_legacy_missing_write_disposition_to_append`.
- `cargo nextest run -p cdf-conformance --locked -E 'test(live_run)' --no-fail-fast`: 7 passed, 54 skipped.
- Final `cargo nextest run -p cdf-contract -p cdf-package -p cdf-engine -p cdf-project -p cdf-conformance --locked`: 202 passed, 0 skipped.

Fast gates:

- `cargo fmt --all -- --check`: pass.
- `cargo check -p cdf-contract -p cdf-package -p cdf-engine -p cdf-project -p cdf-conformance -p cdf-cli --all-targets --locked`: pass.
- `cargo clippy --locked -p cdf-contract -p cdf-package -p cdf-engine -p cdf-project --all-targets -- -D warnings`: pass.
- `git diff --check`: pass before record staging.

Quality and security gates:

- `jscpd --reporters console --no-tips <12 touched Rust files>`: 12 files, 10,732 lines, 92 clones, 859 duplicated lines (8.00%), 6,986 duplicated tokens (9.72%). Findings are dominated by existing and test-helper duplication; no dedup logic clone required abstraction in this slice.
- `rust-code-analysis-cli -m -O json` on `cdf-contract/src/evaluator.rs`, `cdf-engine/src/execution.rs`, and `cdf-project/src/runtime_tests.rs`: reports stored under `target/quality/reports/rust-code-analysis-p1-e3-*`.
  - `evaluator.rs`: SLOC 846, PLOC 791, max cognitive 26 at `evaluate_package_order_dedup`, max cyclomatic 31 at existing `scalar_string`.
  - `execution.rs`: SLOC 508, PLOC 466, max cognitive 25 and max cyclomatic 38 at `execute_to_package_inner`.
  - `runtime_tests.rs`: SLOC 4,075, PLOC 3,653, max cognitive 6 at `package_id_name_rows`, max cyclomatic 8 at `package_replay_stage_name`.
- Direct unsafe/FFI/raw-pointer scan over touched Rust files: no matches.
- `semgrep scan --config p/rust <12 touched Rust files>`: 0 findings from 11 Rust rules.
- `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-p1-e3-<crate>.json <crate>` for `cdf-contract`, `cdf-package`, `cdf-engine`, and `cdf-project`: 0 leaks in each report.
- `cargo geiger --manifest-path "$PWD/crates/<crate>/Cargo.toml" --locked --forbid-only --include-tests --output-format Json --quiet` for `cdf-contract`, `cdf-package`, `cdf-engine`, and `cdf-project`: exit 0. Reports are stored under `target/quality/reports/geiger-p1-e3-*.json`; `cdf-project` reports one dependency package without metrics (`signal-hook-registry`), paired with the clean direct first-party unsafe scan.
- `tools/codeql-rust-quality.sh`: pass, reusable database at `target/quality/codeql-db-rust`, 0 SARIF findings in `target/quality/reports/codeql-rust-current.sarif`. Metrics: 214 Rust files scanned, 0 extraction errors, 3,281 extraction warnings, 4,723 macro calls with 4,607 unresolved. This matches `.10x/knowledge/quality-gate-execution.md`'s known local Rust extractor limitation.
- `cargo vet --locked`: pass, 402 exempted.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: pass after scanning 447 locked crate dependencies.
- `cargo deny check`: pass with known duplicate Arrow 58/59 warnings and final `advisories ok, bans ok, licenses ok, sources ok`.
- `osv-scanner --lockfile Cargo.lock --format json > target/quality/reports/osv-p1-e3-merge-dedup.json`: nonzero only for the already-ratified `RUSTSEC-2024-0436` / `paste` unmaintained advisory.
- `cargo machete --with-metadata ./crates/cdf-contract ./crates/cdf-package ./crates/cdf-engine ./crates/cdf-project`: pass, no unused dependencies in touched crates.
- Full `cargo machete --with-metadata`: reports pre-existing `cdf-cli` unused dependency candidate `cdf-dest-parquet`. It is not introduced by E3 and is now owned by `.10x/tickets/2026-07-08-cdf-cli-unused-parquet-dependency.md`.

## What this supports

- Dedup runs in the live path only for merge plans with an explicit compiled dedup rule.
- Dedup uses accepted package order, not destination state or destination-specific inference.
- `keep = fail` aborts before package finalization and before destination mutation.
- Dedup summaries participate in package identity and can be read during replay/inspection without re-extraction.
- Package replay uses recorded deduped segments and duplicate redrive remains a destination receipt safety rail.
- Existing live-run conformance fixtures remain readable through the `Append` default for legacy `EnginePlan` JSON.

## Limits

This evidence does not close variant capture, trust-ring events, or the drift-quarantine conformance scenario. The dedup live-run integration uses DuckDB; the package-level replay identity and conformance matrix coverage remain broader parent evidence, while E6 will own the aggregate drift-quarantine conformance scenario.
