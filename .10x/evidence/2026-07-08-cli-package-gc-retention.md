Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-07-cli-package-gc-retention.md, .10x/specs/package-lifecycle-determinism.md, .10x/specs/checkpoint-state-commit-gate.md, .10x/specs/project-cli-observability-security.md

# CLI package GC retention evidence

## What was observed

`cdf package gc [DIR]` now produces a dry-run retention plan instead of returning not-supported. The plan classifies package artifacts as `retained`, `collectible`, `missing`, `corrupt`, or `protected`, with stable JSON fields for `package_path`, `package_hash`, `retention_reason`, and `planned_action`.

The implementation preserves proof artifacts by default:

- committed checkpoint package hashes are read through `SqliteCheckpointStore::open_read_only(...).committed_package_hashes()`;
- packages with destination receipts are protected as replay proof;
- archived package tombstones are protected;
- corrupt or partial package directories are retained/fail-closed;
- committed checkpoint hashes whose package artifact is absent are reported as `missing` with `restore_required`;
- no destructive deletion flag or removal behavior was added.

## Procedure

Parent-observed command evidence:

- `cargo fmt --all -- --check`: passed.
- `cargo test -p cdf-cli package_gc --no-default-features`: passed; 2 package-GC tests passed.
- `cargo test -p cdf-state-sqlite sqlite_committed_package_hashes_reports_only_committed_history --locked`: passed; 1 test passed.
- `cargo check -p cdf-cli -p cdf-state-sqlite --no-default-features --locked`: passed.
- `cargo clippy -p cdf-cli -p cdf-state-sqlite --no-default-features --all-targets --locked -- -D warnings`: passed.
- `cargo test -p cdf-cli --no-default-features --locked`: passed; 117 unit tests and 1 integration test passed.
- `cargo test -p cdf-state-sqlite --locked`: passed; 28 tests passed.
- `cargo nextest run -p cdf-cli -p cdf-state-sqlite --locked`: passed; 146 tests passed.
- `git diff --check`: passed.
- `jscpd --reporters console --exit-code 0 --min-lines 6 --min-tokens 60 crates/cdf-cli/src/package_command.rs crates/cdf-cli/src/tests.rs crates/cdf-state-sqlite/src/sqlite.rs crates/cdf-state-sqlite/src/tests.rs`: completed; 4 files, 8,190 lines, 56 clones, 647 duplicated lines, 7.90%.
- `jscpd --reporters console --exit-code 0 --min-lines 6 --min-tokens 60 crates/cdf-cli/src/package_command.rs crates/cdf-state-sqlite/src/sqlite.rs`: completed; 2 production files, 956 lines, 2 clones, 18 duplicated lines, 1.88%.
- `rust-code-analysis-cli -m -O json -p crates/cdf-cli/src/package_command.rs`: passed; top new GC hotspot was `classify_package_artifact` at cyclomatic 14, cognitive 11.
- `rust-code-analysis-cli -m -O json -p crates/cdf-state-sqlite/src/sqlite.rs`: passed; new `committed_package_hashes` measured cyclomatic 6, cognitive 0. Existing `row_to_checkpoint_result` remains the top file hotspot.
- `semgrep scan --no-git-ignore --config p/rust --error --json --output target/quality/reports/semgrep-package-gc.json crates/cdf-cli/src/package_command.rs crates/cdf-cli/src/tests.rs crates/cdf-state-sqlite/src/sqlite.rs crates/cdf-state-sqlite/src/tests.rs`: passed; 0 findings.
- `gitleaks detect --no-git --redact --source crates/cdf-cli/src ...` and `gitleaks detect --no-git --redact --source crates/cdf-state-sqlite/src ...`: passed; no leaks found.
- `rg -n "unsafe|extern \"|\\*const|\\*mut|impl Send|impl Sync" crates/cdf-cli/src/package_command.rs crates/cdf-cli/src/tests.rs crates/cdf-state-sqlite/src/sqlite.rs crates/cdf-state-sqlite/src/tests.rs`: no matches.
- `cargo deny check`: passed; duplicate Arrow warnings remain the already-ratified DuckDB Arrow 58 residual, and final summary was `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo audit --json > target/quality/reports/cargo-audit-package-gc.json`: passed with 0 vulnerabilities; it reports the already-ratified unmaintained `paste` warning.
- `cargo vet --locked`: passed; `Vetting Succeeded (452 exempted)`.
- `cargo machete`: passed; no unused dependencies found.
- `osv-scanner scan source --lockfile Cargo.lock --format json --output-file target/quality/reports/osv-package-gc.json`: exited 1 only for the already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` advisory.
- `tools/codeql-rust-quality.sh`: passed. The reusable database at `target/quality/codeql-db-rust` refreshed because Rust source content changed. SARIF result count was 0. Extraction errors were 0; extraction warnings were 3,516, matching the known local Rust extractor macro-warning profile.
- `cargo semver-checks --package cdf-state-sqlite`: not applicable; the crate has `publish = false` and is not in crates.io, so the tool could not retrieve a registry baseline.

## What this supports

This supports closing `.10x/tickets/done/2026-07-07-cli-package-gc-retention.md` as a dry-run retention planner. The evidence maps to the ticket acceptance criteria:

- retained, collectible, missing, corrupt, and protected classifications are covered by focused CLI tests;
- committed checkpoint package hashes are protected through read-only state history;
- package receipts and retention tombstones are retained;
- corrupt and partial package artifacts fail closed;
- default behavior does not delete artifacts;
- JSON output exposes the required path/hash/reason/action fields.

## Limits

No destructive GC mode was implemented. This is intentional because destructive flag semantics and tombstone/deletion policy beyond dry-run planning were not ratified. The implementation does not classify packages from external object stores or remote state stores; those remain outside this ticket's explicit exclusions.
