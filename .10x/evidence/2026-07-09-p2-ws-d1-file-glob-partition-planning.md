Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-08-p2-ws-d1-file-glob-partition-planning.md, .10x/decisions/data-onramp-file-source-transport-manifest.md, .10x/specs/data-onramp-file-sources-transports.md

# P2 WS-D1 file glob partition planning evidence

## What was observed

`cdf-declarative` now plans local file globs through a shared file-resolution front end. Multi-match local globs produce deterministic per-file partitions. File open and preview both validate and read the path carried by the selected partition rather than independently choosing a single match. The old multi-match runtime rejection text ("narrow the glob to exactly one file") is no longer present in `crates/cdf-declarative/src/file_runtime.rs`.

Each planned partition carries:

- `ScopeKey::File { path }` for the resolved root-relative file path.
- Metadata `kind=files`, `glob=<original glob>`, `resource_id=<compiled resource id>`, `path=<root-relative file path>`, `bytes=<size>`, and `modified_ms=<mtime ms>` when the platform provides it.
- A stable per-path partition id for multi-file matches, while preserving `files` for the existing single-file case.

Parent review found and repaired two determinism holes before closure:

- Multi-file opens now reject the legacy `files` partition id; that compatibility id is accepted only for one-file globs.
- Dynamic absolute source roots are not serialized into partition metadata or scope, so package identity is not tied to temporary test directories.

## Procedure

Focused D1 tests:

```text
cargo test -p cdf-declarative file_glob_ --locked
```

Result: passed. Tests run:

- `tests::file_glob_zero_matches_still_reports_actionable_data_error`
- `tests::file_glob_plans_deterministic_partition_per_match`
- `tests::file_glob_run_and_preview_open_the_requested_partition`

Additional fail-closed path validation test:

```text
cargo test -p cdf-declarative file_runtime_rejects_partition_path_not_produced_by_glob_before_read --locked
cargo test -p cdf-declarative file_runtime_rejects_legacy_partition_id_for_multi_file_glob --locked
```

Result: passed.

Preview/project/conformance compatibility checks after parent repair:

```text
cargo test -p cdf-cli preview_multi_match_file_glob_reads_first_sorted_match_without_writes --locked
cargo test -p cdf-project general_project_run_commits_file_resource --locked
cargo test -p cdf-conformance run_matrix_file_rest_sql_source_cells_persist_output --locked
cargo test -p cdf-conformance live_local_file_duckdb_v1_matches_committed_golden_across_100_runs --locked
cargo test -p cdf-conformance live_local_file_parquet_v1_matches_committed_golden_across_100_runs --locked
cargo test -p cdf-conformance live_local_file_postgres_v1_matches_committed_golden_across_bounded_repeats --locked
```

Result: all passed. The three live local-file golden expected files were updated because `plan/scan.json` and `plan/explain.json` now include `path` and `bytes` partition metadata. The reruns prove the updated hashes are stable across DuckDB and Parquet 100-run golden loops and the bounded Postgres golden loop.

Full crate tests:

```text
cargo test -p cdf-declarative --locked
```

Result: passed. Doctests `0 passed; 0 failed`.

Lint and formatting:

```text
cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings
cargo fmt --all -- --check
git diff --check
```

Result: all passed.

Scoped duplicate and complexity tools over touched Rust files:

```text
jscpd --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/jscpd-p2-d1-scoped crates/cdf-declarative/src/file_runtime.rs crates/cdf-declarative/src/compiled.rs crates/cdf-declarative/src/tests.rs crates/cdf-conformance/src/run_matrix/plan_json.rs crates/cdf-conformance/src/live_run/mod.rs
rust-code-analysis-cli -m -O json -o target/quality/reports/rust-code-analysis-p2-d1-file-runtime -p crates/cdf-declarative/src/file_runtime.rs
rust-code-analysis-cli -m -O json -o target/quality/reports/rust-code-analysis-p2-d1-compiled -p crates/cdf-declarative/src/compiled.rs
rust-code-analysis-cli -m -O json -o target/quality/reports/rust-code-analysis-p2-d1-declarative-tests -p crates/cdf-declarative/src/tests.rs
rust-code-analysis-cli -m -O json -o target/quality/reports/rust-code-analysis-p2-d1-run-matrix-plan-json -p crates/cdf-conformance/src/run_matrix/plan_json.rs
rust-code-analysis-cli -m -O json -o target/quality/reports/rust-code-analysis-p2-d1-live-run-mod -p crates/cdf-conformance/src/live_run/mod.rs
```

Result:

- `jscpd` completed and wrote `target/quality/reports/jscpd-p2-d1-scoped/jscpd-report.json`.
- The scoped report over five touched Rust files reported `20` clones, `284` duplicated lines, `5.40%` duplicated lines, with `newClones: 0` and `newDuplicatedLines: 0`.
- A full-repo inventory also completed at `target/quality/reports/jscpd-p2-d1/jscpd-report.json`, reporting the existing repository baseline of `683` clones, `7,734` duplicated lines, and `6.06%` duplicated lines, again with `newClones: 0` and `newDuplicatedLines: 0`.
- `rust-code-analysis-cli` completed for all five touched Rust files and wrote JSON reports under the five `target/quality/reports/rust-code-analysis-p2-d1-*` directories.

Parent quality sweep:

```text
cargo metadata --format-version 1 --locked
cargo tree --workspace --locked
cargo tree --workspace --locked -d
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked --no-fail-fast
semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-p2-d1.json .
gitleaks detect --source .
gitleaks dir target/quality/source-snap-p2-d1 --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-source-p2-d1.json
cargo deny --locked check
cargo audit --deny warnings --ignore RUSTSEC-2024-0436
cargo vet --locked --no-minimize-exemptions
osv-scanner ...
tools/codeql-rust-quality.sh
```

Result:

- Cargo metadata/tree/check/clippy and the full workspace test suite passed.
- Semgrep initially found `rust.lang.security.temp-dir.temp-dir` in the D1 test helper. The helper was repaired to use `tempfile::TempDir`, and the rerun produced `0` findings.
- Historical Gitleaks scanning still reports only the two exact known false-positive fingerprints documented in `.10x/knowledge/historical-gitleaks-findings.md`. The current-tree source snapshot report at `target/quality/reports/gitleaks-source-p2-d1.json` is empty.
- Supply-chain gates passed. OSV reported only the ratified `RUSTSEC-2024-0436` `paste` advisory already allowed for the DataFusion tuple.
- CodeQL ran through `tools/codeql-rust-quality.sh`, refreshed the reusable database at `target/quality/codeql-db-rust` because Rust/Cargo content changed, and exited `0`. The log is `target/quality/reports/codeql-rust-p2-d1.log`. CodeQL's Rust extractor reported diagnostic macro-resolution noise (`3,926` extraction warnings; `250` Rust files scanned), with no gate failure.

## What this supports

This evidence supports the D1 acceptance criteria:

- Multi-match local globs plan multiple deterministic file partitions.
- File partitions carry file-scoped metadata and `ScopeKey::File` paths suitable for later `FileManifest` work.
- Preview and run use the same partition path validation/open path.
- Existing project/conformance file-run fixtures consume the new partition identity envelope and keep golden package determinism.
- Zero-match globs fail with an actionable data error.
- Single-file behavior remains covered by the full crate suite, including the merge-disposition planning test that still receives a one-file partition.
- No large-N threshold or coalescing behavior was introduced.
- The final quality sweep did not surface a D1 correctness, formatting, lint, security, or supply-chain blocker.

## Limits

This evidence is intentionally scoped to modest-N local files. It does not prove manifest incrementality, compression, remote transports, schema variance policy, no-op reruns, or large-N coalescing. Those remain owned by later WS-D/WS-E children.
