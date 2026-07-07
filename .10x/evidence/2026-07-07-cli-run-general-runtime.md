Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/2026-07-07-cli-run-general-runtime.md, .10x/specs/project-cli-observability-security.md, .10x/specs/run-orchestration-ledger.md

# CLI run general runtime evidence

## What was observed

`cdf run` now constructs `ProjectRunRequest` and calls `cdf_project::run_project` instead of the old `run_local_file_to_duckdb_checkpoint` compatibility path.

The CLI routes local file resources directly, wraps table-backed SQL resources with `SqlRuntimeDependencies` using the existing `ProjectContext` secret provider, and supports `duckdb://` destinations.

REST resources remain fail-closed before package, destination, or checkpoint mutation because no production `HttpTransport` exists in the current crates. Postgres destinations remain fail-closed before package, destination, or checkpoint mutation because `.10x/decisions/project-run-postgres-destination-inputs.md` requires explicit existing-table and merge-dedup policy configuration and no active CLI/project config syntax supplies those values. Filesystem Parquet destinations remain fail-closed because no active record ratifies the CLI URI spelling.

`RunCliReport` now includes `run_id`, destination summary, package fields, checkpoint fields, receipt fields, receipt source, row/segment counts, and a run-ledger event summary.

## Procedure

- `cargo test -p cdf-cli --locked run_ -- --nocapture`: passed, 13 matched tests.
- `cargo test -p cdf-cli --locked`: passed, 70 unit tests, 1 integration test, 0 doc tests.
- `cargo test -p cdf-project --locked general_project_run -- --nocapture`: passed, 15 matched tests.
- `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo nextest run --workspace --locked`: passed, 399 tests.
- `cargo test --doc --workspace --locked`: passed, 17 doc-test binaries with 0 doc tests.
- `cargo hack check --workspace --all-targets --each-feature --locked`: passed for all 17 workspace crates.
- `cargo deny check`: passed; duplicate dependency warnings remain policy warnings.
- `cargo audit`: passed with the already-ratified `RUSTSEC-2024-0436` `paste` unmaintained warning.
- `cargo vet`: passed, 420 exempted.
- `osv-scanner scan source -r . --format json --output target/quality/reports/osv-cli-run-general-runtime.json`: exited non-zero only for the already-ratified `RUSTSEC-2024-0436` `paste` unmaintained warning.
- `semgrep scan --config p/rust --config p/security-audit --error --json --output target/quality/reports/semgrep-cli-run-general-runtime.json crates/cdf-cli/src/commands.rs crates/cdf-cli/src/tests.rs`: passed, 0 findings.
- `tools/codeql-rust-quality.sh`: passed using the reusable database path `target/quality/codeql-db-rust`; the database was refreshed because the source fingerprint changed, analysis produced 0 SARIF results, and extractor warnings matched the known CodeQL Rust macro-expansion limits.
- `rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|\bSend\b|\bSync\b" crates/cdf-cli/src/commands.rs crates/cdf-cli/src/tests.rs`: no matches.
- `git diff --check`: passed.
- `gitleaks protect --staged --redact --report-format json --report-path target/quality/reports/gitleaks-cli-run-general-runtime-staged.json`: passed, no leaks.

## What this supports

- Existing local file to DuckDB CLI behavior remains compatible and now reports the minted run id and ledger event summary.
- Unsupported REST and Parquet CLI combinations fail before package, destination, and checkpoint writes.
- Missing SQL resource secrets fail before run mutation.
- Resolved SQL source secret values are not serialized in CLI stdout/stderr on covered preflight errors.
- Postgres destination secret references are not resolved before the current explicit-policy configuration blocker.
- The lower `cdf-project` general-run facade remains green for local file, REST, SQL, DuckDB, Parquet, and Postgres focused scenarios.

## Limits

CLI Postgres destination success is intentionally unsupported until explicit existing-table and merge-dedup policy configuration is ratified for `cdf run`; live Postgres destination success remains covered at the `cdf-project` layer. CLI REST success is intentionally unsupported until a production HTTP transport is available. CLI filesystem Parquet success is intentionally unsupported until the destination URI spelling is ratified.
