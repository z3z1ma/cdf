Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-a4-generic-discovery-postgres-catalog.md, .10x/tickets/done/2026-07-08-p2-ws-a-discovery-compiler-stage.md

# P2 WS-A4 generic discovery dispatcher and Postgres catalog probe evidence

## What was observed

The A4 slice replaced the direct local-Parquet discovery doorway with a source-archetype project dispatcher and added the first non-file probe for `cdf schema discover <resource>`: declarative Postgres table catalog discovery through the project secret provider.

The implemented probe reads Postgres catalog metadata, maps only the current executable Postgres source type subset, normalizes catalog names through `namecase-v1`, preserves `cdf:source_name` and `cdf:physical_type`, and records schema snapshot metadata without resolving or rendering the DSN secret.

Unsupported source archetypes and unsupported SQL shapes fail closed through the dispatcher instead of falling back to the Parquet path. SQL `plan`/`run` auto-pin remains explicitly outside this child ticket.

## Procedure

Focused functional verification:

```text
cargo test -p cdf-project generic_schema_discovery_dispatch --locked
cargo test -p cdf-dest-postgres catalog --locked
cargo test -p cdf-cli schema_discover --locked
cargo test -p cdf-cli schema_discover_postgres_catalog_uses_project_secret_without_writes_or_secret_leak --locked
cargo test -p cdf-dest-postgres -p cdf-declarative -p cdf-project -p cdf-cli --locked
```

All focused functional commands passed. The Postgres checks used the existing local ephemeral Postgres harness and include catalog type mapping, unsupported catalog type rejection, nullable-state propagation, source/physical metadata, secret redaction, no schema/lock/package/state/destination writes, and CLI JSON/human discovery output.

Workspace verification:

```text
cargo fmt --all -- --check
cargo clippy -p cdf-dest-postgres -p cdf-declarative -p cdf-project -p cdf-cli --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked --no-fail-fast
git diff --check
```

All workspace verification commands passed.

QUALITY tooling:

```text
jscpd --min-lines 12 --min-tokens 80 --reporters console,json --output target/quality/reports/jscpd-a4-impl --format rust --no-colors --exit-code 1 crates/cdf-project/src/schema_discovery.rs crates/cdf-dest-postgres/src/catalog.rs crates/cdf-declarative/src/sql_runtime.rs crates/cdf-cli/src/schema_command.rs crates/cdf-dest-postgres/src/lib.rs crates/cdf-declarative/src/lib.rs
rust-code-analysis-cli -m -p crates/cdf-project/src/schema_discovery.rs -p crates/cdf-project/src/tests.rs -p crates/cdf-dest-postgres/src/catalog.rs -p crates/cdf-dest-postgres/src/tests.rs -p crates/cdf-dest-postgres/src/live_tests.rs -p crates/cdf-declarative/src/sql_runtime.rs -p crates/cdf-declarative/src/lib.rs -p crates/cdf-cli/src/schema_command.rs -p crates/cdf-cli/src/tests.rs -p crates/cdf-dest-postgres/src/lib.rs -O json > target/quality/reports/rust-code-analysis-a4.json
rg -n "\bunsafe\b|extern \"|unsafe impl|impl (Send|Sync)" crates/cdf-project/src/schema_discovery.rs crates/cdf-project/src/tests.rs crates/cdf-dest-postgres/src/catalog.rs crates/cdf-dest-postgres/src/tests.rs crates/cdf-dest-postgres/src/live_tests.rs crates/cdf-declarative/src/sql_runtime.rs crates/cdf-declarative/src/lib.rs crates/cdf-cli/src/schema_command.rs crates/cdf-cli/src/tests.rs crates/cdf-dest-postgres/src/lib.rs
semgrep scan --config p/rust --error --metrics=off --json --output target/quality/reports/semgrep-a4.json crates/cdf-project/src/schema_discovery.rs crates/cdf-project/src/tests.rs crates/cdf-dest-postgres/src/catalog.rs crates/cdf-dest-postgres/src/tests.rs crates/cdf-dest-postgres/src/live_tests.rs crates/cdf-declarative/src/sql_runtime.rs crates/cdf-declarative/src/lib.rs crates/cdf-cli/src/schema_command.rs crates/cdf-cli/src/tests.rs crates/cdf-dest-postgres/src/lib.rs
gitleaks dir crates/cdf-project/src
gitleaks dir crates/cdf-dest-postgres/src
gitleaks dir crates/cdf-declarative/src
gitleaks dir crates/cdf-cli/src
cargo deny check > target/quality/reports/cargo-deny-a4.txt 2>&1
cargo audit --deny warnings --ignore RUSTSEC-2024-0436 --json > target/quality/reports/cargo-audit-a4.json
cargo vet --locked --no-minimize-exemptions > target/quality/reports/cargo-vet-a4.txt 2>&1
cargo machete > target/quality/reports/cargo-machete-a4.txt 2>&1
osv-scanner scan --lockfile Cargo.lock --format json --output target/quality/reports/osv-a4.json
tools/codeql-rust-quality.sh > target/quality/reports/codeql-rust-a4.log 2>&1
```

QUALITY results:

- `jscpd` over implementation files passed with 0 clones and 0 duplicated lines.
- `rust-code-analysis-cli` completed and wrote `target/quality/reports/rust-code-analysis-a4.json`.
- The direct unsafe/FFI scan found no matches in touched Rust files after the CLI test was changed to use `secret://file/sql-dsn` instead of process environment mutation.
- Semgrep completed with 0 findings.
- Gitleaks completed with no leaks in the touched crate source trees.
- `cargo deny`, `cargo audit`, `cargo vet`, and `cargo machete` passed. `cargo audit` used the already-ratified temporary ignore for `RUSTSEC-2024-0436`.
- `osv-scanner` reported only `RUSTSEC-2024-0436`, matching the active ratified exception.
- CodeQL reused the project wrapper and reusable database path `target/quality/codeql-db-rust`. The run completed successfully. Its SARIF findings were the three pre-existing test-fixture `rust/hard-coded-cryptographic-value` findings in `crates/cdf-cli/src/tests.rs`, already owned by `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.

A broader touched-file `jscpd` run that included tests reported duplication in existing test harness scaffolds and assertion blocks. Sampled ranges were large local Postgres/test-project fixtures in `crates/cdf-cli/src/tests.rs`, repeated error-project TOML scaffolds in `crates/cdf-project/src/tests.rs`, and small assertion/setup repetitions in Postgres catalog tests. This was classified as residual test-harness duplication, not an implementation blocker for A4; the implementation-file scan was clean.

## What this supports

- `cdf schema discover <resource>` now reaches a generic project discovery API instead of a Parquet-specific helper.
- Local Parquet discovery remains covered through the dispatcher.
- Declarative Postgres table resources can be discovered through catalog metadata with project secret-provider resolution and without project writes.
- The Postgres discovery slice is source-neutral in shape but deliberately narrow in capability: it only claims types the current Postgres execution path can materialize without new semantics.
- Unsupported REST, arbitrary SQL query resources, and non-Postgres SQL dialects fail closed with source-specific unsupported messages.

## Limits

This evidence does not close SQL `plan`/`run` auto-pin, REST sample-page discovery, Python generator discovery, WASM boundary discovery, Avro-like file discovery, CSV/JSON/NDJSON sampling, remote Parquet ranged discovery, `cdf schema pin|show|diff`, lockfile writes from schema commands, `cdf add`, ad-hoc mode, or S4/S5 conformance closure.
