Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-declarative-postgres-sql-resource-execution.md, .10x/tickets/2026-07-05-conformance-chaos-golden.md

# Declarative Postgres SQL resource execution evidence

## What was observed

Table-backed declarative SQL resources for `dialect = "postgres"` now open through explicit runtime dependencies and execute as `ResourceStream`s against the existing local Postgres live harness. The default compiled SQL resource still fails closed without runtime dependencies. Arbitrary SQL `query` resources, non-Postgres dialects, malformed table metadata, missing or empty connection secrets, empty or unsupported schemas, tampered partition metadata, unstructured predicates, cursor projection gaps, NULL cursor values, and mismatched runtime row types fail closed.

The implementation preserves the public conformance API: an attempted public cursor requirement addition was removed after `cargo semver-checks` identified it as an API break. Cursor source-position emission is asserted directly in live Postgres source coverage.

## Procedure

Focused implementation checks:

- `cargo fmt --all` passed.
- `cargo fmt --all -- --check` passed.
- `git diff --check -- . ':(exclude).gitignore'` passed.
- `cargo test -p cdf-dest-postgres --locked --no-fail-fast` passed with 25 tests, including live local Postgres source execution.
- `cargo test -p cdf-declarative --locked --no-fail-fast` passed with 48 tests.
- `cargo test -p cdf-conformance --locked resource -- --nocapture` passed.
- `cargo clippy -p cdf-dest-postgres -p cdf-declarative -p cdf-conformance --all-targets --locked -- -D warnings` passed.
- `cargo nextest run -p cdf-dest-postgres -p cdf-declarative -p cdf-conformance --locked` passed with 114 tests.
- `cargo semver-checks check-release -p cdf-dest-postgres --baseline-rev HEAD` passed.
- `cargo semver-checks check-release -p cdf-declarative --baseline-rev HEAD` passed.
- `cargo semver-checks check-release -p cdf-conformance --baseline-rev HEAD` passed after removing the public conformance enum variant.

Broader `QUALITY.md` checks:

- Tool availability was checked for the configured quality suite: `cargo-nextest`, `cargo-llvm-cov`, `cargo-hack`, `cargo-deny`, `cargo-audit`, `cargo-vet`, `cargo-machete`, `cargo-udeps`, `cargo-semver-checks`, `cargo-geiger`, `cargo-mutants`, `rust-code-analysis-cli`, `semgrep`, `osv-scanner`, `gitleaks`, and `jscpd`.
- `cargo check --workspace --all-targets --locked` passed.
- `cargo test --workspace --locked --no-fail-fast` passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` passed.
- `cargo doc --workspace --no-deps --locked` passed.
- `cargo hack check --workspace --all-targets --each-feature --locked` passed.
- `cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings` passed; report stored at `target/quality/reports/cargo-hack-clippy-postgres-sql-source.txt`.
- `cargo llvm-cov --workspace --all-features --locked --summary-only` passed; report stored at `target/quality/reports/llvm-cov-postgres-sql-source.txt`. Workspace line coverage was 81.79%. `cdf-dest-postgres/src/source.rs`, `cdf-declarative/src/sql_runtime.rs`, and `cdf-conformance/src/resource/execution.rs` were included in the report.
- `cargo deny check` passed; report stored at `target/quality/reports/deny-postgres-sql-source.txt`.
- `cargo audit --json` passed with 0 vulnerabilities; report stored at `target/quality/reports/cargo-audit-postgres-sql-source.json`.
- `cargo vet --locked --output-format json` passed with conclusion `success`; report stored at `target/quality/reports/cargo-vet-postgres-sql-source.json`.
- `osv-scanner --lockfile Cargo.lock` returned only the ratified `RUSTSEC-2024-0436` `paste 1.0.15` advisory; report stored at `target/quality/reports/osv-postgres-sql-source.json`.
- `semgrep scan --config auto --error` over touched crate source directories passed with 0 findings; report stored at `target/quality/reports/semgrep-postgres-sql-source.json`.
- `cargo machete --with-metadata` passed with no unused dependencies reported.
- `cargo +nightly udeps -p cdf-dest-postgres -p cdf-declarative -p cdf-conformance --all-targets --locked` passed; report stored at `target/quality/reports/udeps-postgres-sql-source.txt`.
- `gitleaks dir` passed for `crates/cdf-dest-postgres`, `crates/cdf-declarative`, and `crates/cdf-conformance`; reports stored under `target/quality/reports/gitleaks-postgres-sql-*.json`.
- A bounded snapshot of relevant `.10x` records was scanned with `gitleaks dir` and passed; report stored at `target/quality/reports/gitleaks-postgres-sql-records.json`.
- Direct first-party unsafe/concurrency scan over touched source directories found no `unsafe` blocks, `extern` blocks, raw pointers, or manual `Send`/`Sync` implementations in the new implementation surface. Hits were `Send`/`Sync` trait bounds, `Mutex` in existing REST/tests/live harness code, `NoTls` Postgres connection usage, and test identifiers named `unsafe`; report stored at `target/quality/reports/unsafe-scan-postgres-sql-source.txt`.
- `rust-code-analysis-cli` produced JSON metric files for 24 touched-surface Rust files under `target/quality/reports/rca-postgres-sql-*`.
- `jscpd` over touched source directories passed the configured 10% threshold with 4.90% duplicated lines; report stored at `target/quality/reports/jscpd-postgres-sql-source.txt`.
- Bounded `cargo mutants` over `crates/cdf-dest-postgres/src/source.rs` predicate/resource/cursor/redaction paths completed with 12 selected mutants: 9 caught, 3 unviable, 0 missed, 0 timed out. Report stored at `target/quality/mutants-postgres-sql-source-in-place/mutants.out/`.

Known tool limits and skips:

- CodeQL was skipped for this checkpoint per the active goal instruction not to recreate the CodeQL database.
- Full dependency `cargo geiger` and `cargo geiger --forbid-only` were attempted against the touched crates but hung without producing reports and were interrupted. The direct first-party unsafe scan is the soundness evidence for this slice; this evidence does not claim a fresh `geiger` dependency census.
- `cargo miri`, `cargo careful`, fuzzing, Kani, benchmarks, and bloat profiling were not run because this slice does not add unsafe code, concurrency algorithms, or a benchmarked performance path, and no existing harness for those tools owns the changed behavior.

## What this supports or challenges

This supports the acceptance criteria for table-backed declarative Postgres SQL resources: explicit secret-provider runtime opening, default fail-closed behavior, typed pushdown planning, safe SQL construction through validated identifiers and driver parameters, live Postgres batch execution, correct batch metadata, cursor source positions, conformance coverage, and source-compatible public APIs.

The evidence also supports the parent conformance track's source-execution breadth: file, REST, and table-backed Postgres SQL resources now have openable execution coverage, while CLI `run` widening and full general orchestration remain outside this child.

## Limits

This evidence does not cover arbitrary declarative SQL `query` execution, non-Postgres dialects, CLI `cdf run` SQL execution, package/checkpoint orchestration for SQL resources, CDC, connection pooling, DataFusion `TableProvider` delegation, hosted database providers, or general run-ledger semantics.
