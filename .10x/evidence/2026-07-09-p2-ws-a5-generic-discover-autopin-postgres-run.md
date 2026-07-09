Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-a5-generic-discover-autopin-postgres-run.md

# P2 WS-A5 generic discover auto-pin for Postgres run and plan evidence

## What was observed

The A5 implementation replaces CLI plan/preview/run local-Parquet-only discover preparation with generic discover preparation through `prepare_discover_resource`, preserves local Parquet auto-pin behavior, and allows declarative Postgres table resources in discover mode to auto-pin through the A4 catalog dispatcher before package-producing execution.

Pinned `SchemaSource::Discovered` Postgres table sources are accepted for execution with the snapshot schema hash. Unpinned `Discover`, `Hints`, and `Contract` source modes remain rejected by the Postgres source execution gate.

Postgres source query generation uses `cdf:source_name` metadata for physical column references while preserving the normalized Arrow field names in emitted batches. Focused SQL generation and live Postgres execution both proved a physical `"VendorID"` source column can be read as emitted `vendor_id`.

The CLI Postgres discover-mode integration exercised `cdf plan`, `cdf preview`, and `cdf run` against a live Postgres table using `secret://file/sql-dsn`. The test asserted no resolved DSN or password leakage, verified the Postgres catalog snapshot metadata, verified preview reads, verified run checkpoint schema hash matches the snapshot hash, and verified DuckDB output columns use the normalized `vendor_id` field.

## Procedure

Focused tests:

```text
cargo test -p cdf-project generic_discover_prepare_preserves_local_parquet_autopin_behavior --locked
cargo test -p cdf-project generic_schema_discovery_dispatch_preserves_local_parquet_behavior_without_writes --locked
cargo test -p cdf-dest-postgres source_shape_accepts_discovered_snapshot_and_rejects_unpinned_schema_modes --locked
cargo test -p cdf-dest-postgres query_builder_uses_source_name_metadata_for_physical_columns --locked
cargo test -p cdf-dest-postgres live_postgres_table_resource_reads_source_name_physical_columns --locked
cargo test -p cdf-cli postgres_discover_mode_plan_preview_run_autopins_through_file_secret_without_leaks --locked
cargo test -p cdf-cli plan_local_parquet_discover_autopins_snapshot_and_reports_hash --locked
cargo test -p cdf-cli run_local_parquet_discover_autopins_and_commits_pinned_schema --locked
cargo test -p cdf-cli run_unsupported_discover_schema_resource_fails_before_writes --locked
cargo test -p cdf-cli preview_sql_table_resource_uses_postgres_runtime_without_writes --locked
cargo test -p cdf-cli run_sql_resource_with_ordered_cursor_commits_checkpoint --locked
cargo test -p cdf-cli schema_discover_postgres_catalog_uses_project_secret_without_writes_or_secret_leak --locked
```

All focused tests passed.

Quality and affected-crate verification:

```text
cargo check -p cdf-project -p cdf-cli -p cdf-dest-postgres --tests --locked
cargo fmt --all
cargo fmt --all -- --check
git diff --check
cargo clippy -p cdf-project -p cdf-dest-postgres -p cdf-cli --all-targets --locked -- -D warnings
cargo test -p cdf-project -p cdf-dest-postgres -p cdf-cli --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked --no-fail-fast
```

All quality, affected-crate, and workspace verification commands passed. The broad affected-crate test run completed with:

- `cdf-cli`: 205 tests passed, plus `doctor_env` integration test passed.
- `cdf-dest-postgres`: 34 tests passed.
- `cdf-project`: 103 tests passed.
- Doc-tests for `cdf-cli`, `cdf-dest-postgres`, and `cdf-project`: 0 tests, passed.

The full workspace test run also passed, including conformance, golden, destination, project, engine, format, state, Python, subprocess, and doctest suites.

QUALITY tooling:

```text
jscpd --min-lines 12 --min-tokens 80 --reporters console,json --output target/quality/reports/jscpd-a5-impl --format rust --no-colors --exit-code 1 crates/cdf-project/src/schema_discovery.rs crates/cdf-cli/src/run_command.rs crates/cdf-cli/src/scan_command.rs crates/cdf-dest-postgres/src/source.rs
jscpd --min-lines 12 --min-tokens 80 --reporters console,json --output target/quality/reports/jscpd-a5-all --format rust --no-colors --exit-code 1 crates/cdf-project/src/schema_discovery.rs crates/cdf-project/src/tests.rs crates/cdf-cli/src/run_command.rs crates/cdf-cli/src/scan_command.rs crates/cdf-cli/src/tests.rs crates/cdf-dest-postgres/src/source.rs crates/cdf-dest-postgres/src/live_tests.rs
rust-code-analysis-cli -m -p crates/cdf-project/src/schema_discovery.rs -p crates/cdf-project/src/tests.rs -p crates/cdf-cli/src/run_command.rs -p crates/cdf-cli/src/scan_command.rs -p crates/cdf-cli/src/tests.rs -p crates/cdf-dest-postgres/src/source.rs -p crates/cdf-dest-postgres/src/live_tests.rs -O json > target/quality/reports/rust-code-analysis-a5.json
semgrep scan --config p/rust --error --metrics=off --json --output target/quality/reports/semgrep-a5.json crates/cdf-project/src/schema_discovery.rs crates/cdf-project/src/tests.rs crates/cdf-cli/src/run_command.rs crates/cdf-cli/src/scan_command.rs crates/cdf-cli/src/tests.rs crates/cdf-dest-postgres/src/source.rs crates/cdf-dest-postgres/src/live_tests.rs
rg -n "\bunsafe\b|extern \"|unsafe impl|impl (Send|Sync)" crates/cdf-project/src/schema_discovery.rs crates/cdf-project/src/tests.rs crates/cdf-cli/src/run_command.rs crates/cdf-cli/src/scan_command.rs crates/cdf-cli/src/tests.rs crates/cdf-dest-postgres/src/source.rs crates/cdf-dest-postgres/src/live_tests.rs
gitleaks dir crates/cdf-project/src
gitleaks dir crates/cdf-cli/src
gitleaks dir crates/cdf-dest-postgres/src
cargo deny check > target/quality/reports/cargo-deny-a5.txt 2>&1
cargo audit --deny warnings --ignore RUSTSEC-2024-0436 --json > target/quality/reports/cargo-audit-a5.json
cargo vet --locked --no-minimize-exemptions > target/quality/reports/cargo-vet-a5.txt 2>&1
cargo machete > target/quality/reports/cargo-machete-a5.txt 2>&1
osv-scanner scan --lockfile Cargo.lock --format json --output target/quality/reports/osv-a5.json
tools/codeql-rust-quality.sh > target/quality/reports/codeql-rust-a5.log 2>&1
```

QUALITY results:

- Implementation-only `jscpd` passed with 0 clones and 0 duplicated lines.
- Broad touched-file `jscpd` reported 22 clones and 363 duplicated lines across test files and shared fixture scaffolds. Sampled ranges match existing CLI/Postgres test harness repetition; implementation files remain clone-free, and this residual is not an A5 blocker.
- `rust-code-analysis-cli` completed and wrote `target/quality/reports/rust-code-analysis-a5.json`.
- The direct unsafe/FFI grep found only the string literal `"unsafe"` in an existing test predicate id, not a Rust `unsafe` construct.
- Semgrep completed with 0 findings.
- Gitleaks completed without leaks in touched source trees.
- `cargo deny`, `cargo audit`, `cargo vet`, and `cargo machete` passed. `cargo audit` used the already-ratified temporary ignore for `RUSTSEC-2024-0436`.
- `osv-scanner` reported only `RUSTSEC-2024-0436`, matching the active ratified exception.
- CodeQL completed through `tools/codeql-rust-quality.sh`, using the reusable `target/quality/codeql-db-rust` path. SARIF findings were the same three pre-existing `rust/hard-coded-cryptographic-value` findings in `crates/cdf-cli/src/tests.rs` lines 1319, 1409, and 1465, already owned by `.10x/tickets/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.

An initial focused `cargo test` invocation attempted multiple test-name filters in one command and Cargo rejected the command shape. The individual focused test filters were rerun correctly and passed.

## What this supports

- CLI plan, preview, and run use the generic discover preparer and pinned compiled resource for discover-mode resources.
- Local Parquet discover auto-pin remains deterministic and covered.
- Postgres discover-mode table resources can auto-pin from catalog metadata and execute through plan, preview, and run.
- The run path uses the pinned compiled resource for both engine planning and runtime resource opening; run reports and checkpoint state use the discovered snapshot hash.
- Resolved Postgres DSNs and password-bearing URI values did not appear in tested CLI outputs or snapshot artifacts.
- Unsupported discover-mode NDJSON and multi-file Parquet slices still fail before package, destination, checkpoint, and schema snapshot writes where applicable.

## Limits

This evidence does not close REST sample-page discovery, Python generator discovery, WASM boundary discovery, Avro-like file discovery, CSV/JSON/NDJSON sampling, remote Parquet ranged discovery, multi-file schema union/variance, `cdf schema pin|show|diff`, `cdf add`, ad-hoc mode, or S4/S5 conformance closure.
