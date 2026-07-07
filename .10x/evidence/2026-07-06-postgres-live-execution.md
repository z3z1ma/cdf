Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-postgres-live-execution.md, .10x/tickets/done/2026-07-05-postgres-destination.md, .10x/specs/destination-receipts-guarantees.md, .10x/specs/package-lifecycle-determinism.md

# Postgres live execution evidence

## What was observed

`crates/cdf-dest-postgres` now has a driver-backed live commit path:

- `PostgresDestination::connect`, `commit_package`, and `verify_receipt` use the synchronous `postgres` driver.
- Live commits consume the existing `PostgresLoadPlan`; no parallel planning model was introduced.
- Package loading uses `cdf-package::PackageReader`, verifies package identity, checks replay package hash, validates complete segment coverage, enforces one schema across segments, checks Arrow schema and Postgres column plan agreement, and verifies manifest row counts.
- Rows stage through Postgres `COPY ... FORMAT csv` into a temporary table with `_cdf_load`, `_cdf_segment`, `_cdf_row`, and `_cdf_loaded_at_ms` system columns.
- Append, replace, and merge execute inside one Postgres transaction. Merge uses the existing deterministic `MergeDedupPolicy` behavior.
- `_cdf_loads` and `_cdf_state` are created and populated transactionally with the target write. For schema-qualified targets, the transaction sets `search_path` so mirrors live with the target schema instead of leaking into `public`.
- Duplicate package detection now matches the `_cdf_loads` uniqueness key, `(target, package_hash)`, and returns the stored receipt without rewriting target rows.
- Receipts include xid metadata, counts, segment acknowledgements, schema hash, migrations, idempotency token, and a `postgres_sql` verify clause. Verification succeeds both inside the commit transaction and from a fresh connection.
- Decimal128 and Decimal256 live row encoding now matches the exact `NUMERIC(p,s)` sheet contract.
- Package receipt append after a durable DB commit is best-effort in the Postgres API: a package-side recording error returns a committed `PostgresCommitOutcome` with `package_receipt_error`, avoiding a false committed-but-`Err` outcome.

The crate remains non-monolithic: `src/lib.rs` only declares modules and re-exports the public API; implementation lives in focused modules including `api.rs`, `commit.rs`, `ddl.rs`, `dml.rs`, `mirrors.rs`, `package.rs`, `rows.rs`, `sheet.rs`, and tests.

## Procedure

Focused Postgres checks:

```text
cargo fmt --all
cargo check -p cdf-dest-postgres --all-targets --locked
cargo test -p cdf-dest-postgres --locked --no-fail-fast
cargo clippy -p cdf-dest-postgres --all-targets --locked -- -D warnings
```

Result: passed. `cdf-dest-postgres` ran 17 tests, including live ephemeral Postgres tests for append/duplicate/receipt verification/state mirror, replace delete counts, merge last-row dedup and update counts, Decimal128 exact numeric writes, package receipt append failure after durable DB commit, and rollback after stage copy.

Final workspace correctness and feature gates:

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo check --workspace --all-targets --all-features --locked
cargo check --workspace --all-targets --no-default-features --locked
cargo test --workspace --all-targets --locked --no-fail-fast
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings
cargo hack check --workspace --all-targets --each-feature --locked
cargo nextest run --workspace --locked
cargo doc --workspace --all-features --no-deps --locked
git diff --check
```

Result: passed. `nextest` reported 243 tests passed. Reports are under `target/quality/reports/*postgres-live-final*`.

Security and supply-chain gates:

```text
cargo deny check
cargo audit --deny warnings
cargo vet --locked --output-format=json
osv-scanner --lockfile=Cargo.lock --format=json
cargo machete --with-metadata
semgrep scan --config=auto --error --json --output target/quality/reports/semgrep-postgres-live-final.json crates/cdf-dest-postgres
gitleaks detect --source crates/cdf-dest-postgres --no-git --report-format json --report-path target/quality/reports/gitleaks-postgres-crate-final.json
gitleaks detect --source supply-chain --no-git --report-format json --report-path target/quality/reports/gitleaks-supply-chain-final.json
gitleaks detect --pipe --report-format json --report-path target/quality/reports/gitleaks-cargo-lock-final.json < Cargo.lock
rg -n "unsafe\b|extern \"|raw pointer|\*const|\*mut|Send for|Sync for" crates/cdf-dest-postgres/src crates/cdf-dest-postgres/Cargo.toml
```

Result: passed. `cargo vet` initially failed because the new Postgres driver dependency tree was unvetted; `cargo vet regenerate exemptions --locked` added explicit current-version `safe-to-deploy` exemptions in `supply-chain/config.toml`, after which `cargo vet --locked` and JSON output concluded `success`. OSV, Semgrep, gitleaks, and the direct first-party unsafe/FFI scan found no findings.

CodeQL:

```text
tools/codeql-rust-quality.sh
```

Result: passed with SARIF result count 0 at `target/quality/reports/codeql-rust-current.sarif`. The stale-aware wrapper reused the durable path `target/quality/codeql-db-rust`; because Rust source, manifests, and lockfile changed, it refreshed that reusable DB in place rather than creating a disposable DB elsewhere.

Coverage and compatibility:

```text
cargo llvm-cov -p cdf-dest-postgres --locked --summary-only
cargo semver-checks check-release --package cdf-dest-postgres --baseline-root .
rust-code-analysis-cli -m -p crates/cdf-dest-postgres/src -O json -o target/quality/reports/rust-code-analysis-postgres-live-final
```

Result: passed. `cargo llvm-cov` reported `cdf-dest-postgres` line coverage of 64.88%. `cargo semver-checks` reported 196 checks passed and no semver update required.

Geiger:

```text
cargo geiger --manifest-path /Users/alexanderbut/code_projects/personal/cdf/crates/cdf-dest-postgres/Cargo.toml --all-targets --locked
cargo geiger --manifest-path /Users/alexanderbut/code_projects/personal/cdf/crates/cdf-dest-postgres/Cargo.toml --forbid-only --all-targets --locked
```

Result: not used as closure evidence. The full Geiger run fell into a native `libduckdb-sys` build through the existing `cdf-package` dependency path and was stopped. The bounded `--forbid-only` run repeatedly hit a third-party `signal-hook-registry` parse failure and was stopped. The closure soundness evidence for this slice is the direct first-party unsafe/FFI scan plus the other static/security gates.

## What this supports or challenges

This supports closing `.10x/tickets/done/2026-07-06-postgres-live-execution.md`: every acceptance criterion has direct implementation and live integration evidence.

This also resolves the blocker on `.10x/tickets/done/2026-07-05-postgres-destination.md`: the earlier destination sheet/SQL/receipt/drift-hook evidence is now complemented by live append, replace, merge, duplicate, receipt verification, mirror, rollback, and decimal execution evidence.

## Limits

Tests use an ephemeral local Postgres server started from local `initdb`/`pg_ctl`, or `TEST_DATABASE_URL` if explicitly supplied. They do not exercise Docker. `cargo-geiger` was not a successful gate for the reasons above. CodeQL reported extractor diagnostic limitations for Rust macro expansion but produced zero SARIF findings.
