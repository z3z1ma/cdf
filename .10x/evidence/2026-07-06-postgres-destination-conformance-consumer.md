Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-postgres-destination-conformance-consumer.md, .10x/tickets/2026-07-05-conformance-chaos-golden.md, .10x/specs/conformance-governance-roadmap.md, .10x/specs/destination-receipts-guarantees.md

# Postgres destination conformance consumer evidence

## What was observed

`cdf-dest-postgres` now consumes the reusable destination conformance harness through `cdf-conformance` as a dev-dependency. The new Postgres consumer test covers the three dispositions declared by the Postgres destination sheet: append, replace, and merge.

The test calls `PostgresDestination::plan_commit` through the public `DestinationProtocol` surface and passes the returned system-table migrations into `DestinationConformanceCase::with_expected_migrations`, so it does not make an empty-migration assumption. It also asserts the expected `_cdf_loads` and `_cdf_state` migration identifiers are present before the reusable harness checks the plan.

No Postgres production behavior, reusable conformance harness behavior, CLI surface, CDC behavior, or chaos behavior changed.

## Procedure and results

Implementation and focused tests:

- `cargo metadata --format-version=1 --offline` updated `Cargo.lock` offline after adding the local `cdf-conformance` dev-dependency.
- `cargo metadata --format-version=1 --locked` passed after the lockfile update.
- `cargo test -p cdf-dest-postgres --locked tests::reusable_destination_conformance_suite_accepts_postgres_sheet_and_plans -- --exact` passed with 1 test.
- `cargo test -p cdf-dest-postgres --locked live_ -- --nocapture` passed with 6 tests. Output showed local `initdb`/`pg_ctl` database initialization, server start, server stop, and all six `live_tests::live_*` tests passing.
- `cargo test -p cdf-dest-postgres --locked --no-fail-fast` passed with 18 tests and 0 failures, including the six live Postgres tests.
- `cargo test -p cdf-conformance --locked --no-fail-fast` passed with 27 tests and 0 failures.
- `cargo nextest run -p cdf-dest-postgres -p cdf-conformance --locked` passed with 45 tests run, 45 passed, 0 skipped.
- `cargo check -p cdf-dest-postgres -p cdf-conformance --all-targets --locked` passed.
- `cargo clippy -p cdf-dest-postgres --all-targets --locked -- -D warnings` passed.
- `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings` passed.
- `cargo fmt --all -- --check` passed.
- `git diff --check -- . ':(exclude).gitignore'` passed.

Supply-chain, static analysis, and dependency hygiene:

- `cargo deny check` passed; duplicate-version warnings were emitted before final `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo audit` passed after scanning 429 crate dependencies.
- `cargo vet --locked` passed with the current exemption backlog.
- `osv-scanner scan source -r --format json --output-file target/quality/osv-postgres-conformance.json .` passed; JSON summary reported no vulnerabilities.
- `semgrep scan --config p/rust --error --json --output target/quality/semgrep-postgres-conformance.json crates/cdf-dest-postgres crates/cdf-conformance` passed with 0 findings.
- `tools/codeql-rust-quality.sh` passed. It refreshed the reusable database at `target/quality/codeql-db-rust` because Rust source/manifest/lockfile content changed, then produced `target/quality/reports/codeql-rust-current.sarif` with 0 results.
- `gitleaks git --redact --report-format json --report-path target/quality/gitleaks-postgres-conformance-git.json .` passed with no leaks.
- A source-only `gitleaks dir` scan over a temporary mirror of `git ls-files`, excluding the unrelated `.gitignore` worktree change, passed with no leaks and wrote `target/quality/gitleaks-postgres-conformance-source.json`.
- `rg -n '\bunsafe\b|extern "|raw pointer|\*const|\*mut|unsafe impl (Send|Sync)' crates/cdf-dest-postgres crates/cdf-conformance` found no first-party unsafe, FFI, raw-pointer, or unsafe Send/Sync surfaces.
- `cargo machete --with-metadata` passed with no unused dependencies.
- `cargo +nightly udeps -p cdf-dest-postgres -p cdf-conformance --all-targets --locked` passed; all dependencies appeared used.

## What this supports or challenges

This supports closing `.10x/tickets/done/2026-07-06-postgres-destination-conformance-consumer.md`: Postgres now participates in the reusable destination conformance harness at the same planning/sheet level as DuckDB and Parquet, and its existing live execution tests still provide runtime evidence for commit, duplicate/no-op replay, receipt verification, mirrors, rollback, and decimal fidelity.

The evidence also supports progress on `.10x/tickets/2026-07-05-conformance-chaos-golden.md`: the previously open live Postgres destination conformance gap is now covered at the planning harness level, while broader lifecycle chaos and MVP demo work remain separate parent scope.

## Limits

Mutation testing was not run for this slice because no reusable conformance harness logic or production code changed. The existing destination harness mutation evidence remains the oracle for harness behavior; this slice adds a downstream consumer test proving Postgres conforms to that already-mutated harness.

CodeQL reported the known Rust extractor macro-warning profile, but SARIF contained 0 findings. The local `.gitignore` modification was pre-existing and intentionally excluded from this slice.
