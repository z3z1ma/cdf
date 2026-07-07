Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-general-run-postgres-destination.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/destination-receipts-guarantees.md, .10x/decisions/project-run-postgres-destination-inputs.md

# General run Postgres destination evidence

## What was observed

The general project-run runtime now has a Postgres destination request shape with explicit `database_url`, `PostgresTarget`, merge dedup policy, and optional existing-table policy. `ProjectRunDestination` debug output redacts the database URL.

The implementation preflights Postgres load-plan shape before package, state, destination, or checkpoint mutation. The full `PostgresLoadPlanInput` is constructed after package finalization, using finalized package schema columns, package hash, package segments, explicit target/dedup/existing-table inputs, and descriptor merge keys when present. No destination introspection supplies missing semantics.

Postgres column derivation is destination-owned through `cdf-dest-postgres::postgres_columns_for_schema`, so the project runtime does not duplicate Arrow-to-Postgres type mapping.

Package replay routes through `PostgresDestination::with_commit_request(...).begin(...)`, applies migrations, writes, finalizes to a receipt, verifies through `PostgresDestination::verify_receipt`, then commits the checkpoint and updates package status. Because generic `CommitSession::finalize` returns only `Receipt`, project-run Postgres reports use receipt-only destination metadata and do not claim duplicate/no-op status.

Crash recovery from a durable Postgres receipt verifies the receipt, commits or reuses the checkpoint, and updates package status from package/receipt/checkpoint artifacts without receiving or contacting a source resource.

## Procedure

- `cargo fmt --all -- --check`
  - Result: passed.
- `cargo test -p cdf-project --locked postgres -- --nocapture`
  - Result: passed, 4 tests.
  - Covered the new Postgres project-run success/order path, unsupported Postgres preflight rejection before mutation, durable-receipt recovery without source contact, and the pre-existing Postgres SQL source runtime test.
- `cargo test -p cdf-dest-postgres --locked --no-fail-fast`
  - Result: passed, 27 unit/live tests and 0 doctests.
  - Local Postgres harness was available.
- `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`
  - Result: passed.
- `cargo clippy -p cdf-project -p cdf-dest-postgres --all-targets --locked -- -D warnings`
  - Result: passed after parent review moved Postgres schema mapping ownership into `cdf-dest-postgres`.
- `cargo check --workspace --all-targets --locked`
  - Result: passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast`
  - Result: passed. Workspace unit/integration tests included the live Postgres destination tests and `cdf-project` runtime tests.
- `cargo deny check`
  - Result: passed. The command emitted duplicate-version warnings for the known Arrow/DataFusion tuple, but advisories, bans, licenses, and sources were ok.
- `cargo vet`
  - Result: passed: `Vetting Succeeded (420 exempted)`.
- `cargo audit --json`
  - Result: no vulnerabilities. The only warning was the ratified informational unmaintained advisory `RUSTSEC-2024-0436` for `paste 1.0.15`.
- `osv-scanner scan source -r . --format json --output target/quality/reports/osv-general-run-postgres.json`
  - Result: report contained one vulnerability, the ratified `RUSTSEC-2024-0436` `paste` advisory.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-general-run-postgres.json .`
  - Result: report contained 0 results and 0 errors.
- `codeql database analyze target/quality/codeql-db-rust codeql/rust-queries --format=sarif-latest --output=target/quality/reports/codeql-general-run-postgres-existing-db.sarif --rerun`
  - Result: report contained 0 query results. Limit: this reused the existing CodeQL database per instruction and reported one Rust extractor quality notification (`Ill-formed type mention`); no CodeQL database was recreated for this change.
- `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-general-run-postgres.json .`
  - Result: broad directory scan reported hits only under generated `target/` build output from vendored DuckDB/mbedTLS sources. This is not source/commit-surface evidence; staged secret scanning is required before commit.
- `gitleaks git --staged --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-general-run-postgres-staged.json .`
  - Result: passed, no leaks found.
- `git diff --check`
  - Result: passed.
- `git diff --cached --check`
  - Result: passed after staging the owned source and `.10x/` changes.

## What this supports or challenges

Supports all acceptance criteria on `.10x/tickets/done/2026-07-07-general-run-postgres-destination.md`:

- Supported packages commit to Postgres through the destination session path.
- Postgres project runs record the same run-ledger event order as DuckDB and Parquet project-run paths.
- Postgres receipt verification happens before checkpoint commit.
- Unsupported Postgres schema/type combinations fail before source/package/destination/checkpoint mutation.
- Durable-receipt recovery commits checkpoint state and updates package status without source contact.

## Limits

The `ProjectReceiptSource` for Postgres does not expose duplicate/no-op metadata because the generic commit session finalizer returns only a `Receipt`; this is intentional rather than inferred. Duplicate receipt behavior remains covered by `cdf-dest-postgres` destination tests.

Existing concurrent changes to `.gitignore` and the untracked `cdf-repo-selection-2026-07-07.zip` were not touched.

The CodeQL analysis used the reusable database at `target/quality/codeql-db-rust`; it did not rebuild the database. Because source files changed after the database was created, this CodeQL result is a reused-database signal with extractor-quality limits, not a fresh extraction proof for the current diff.
