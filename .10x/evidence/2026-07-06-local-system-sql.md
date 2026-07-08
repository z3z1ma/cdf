Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-local-system-sql.md, .10x/tickets/done/2026-07-05-cli-surface.md, .10x/tickets/2026-07-05-observability-doctor-status-sql.md, .10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md, .10x/tickets/done/2026-07-06-improve-codeql-rust-extractor-coverage.md

# Local System SQL Evidence

## What was observed

`cdf sql <query>` now supports the first local system-history SQL surface. The CLI loads the project, builds an in-memory SQLite database, mounts checkpoint rows from the configured SQLite state file when present, scans package manifests and package receipts from the configured package root, and evaluates one read-only query.

Queryable tables:

- `checkpoints`
- `packages`
- `package_files`
- `package_segments`
- `package_receipts`
- `package_receipt_segments`

The JSON output is stable and contains `tables`, `columns`, and `rows`. Human output is concise: `sql returned N row(s) from local system history`.

Read-only protection has two layers:

- A conservative lexical gate accepts only one `SELECT` or `WITH` statement, strips one trailing semicolon, skips strings/comments, and rejects obvious mutating keywords such as `insert`, `update`, `delete`, `create`, `drop`, `alter`, `pragma`, `attach`, `detach`, `vacuum`, `reindex`, and `replace`.
- After preparing the in-memory statement, `rusqlite::Statement::readonly()` must report true before execution.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/cdf`:

```text
cargo fmt --all -- --check
git diff --check -- . ':(exclude).gitignore'
cargo check --workspace --all-targets --locked
cargo check --workspace --all-targets --all-features --locked
cargo check --workspace --all-targets --no-default-features --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings
cargo test -p cdf-cli --locked --no-fail-fast
cargo test --workspace --all-targets --locked --no-fail-fast
cargo test --workspace --all-targets --all-features --locked --no-fail-fast
cargo test --workspace --doc --all-features --locked --no-fail-fast
cargo doc --workspace --all-features --no-deps --locked
cargo nextest run --workspace --locked
cargo hack check --workspace --all-targets --each-feature --locked
cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings
cargo llvm-cov --workspace --all-features --locked --summary-only
cargo machete
cargo audit --json
cargo deny check advisories
cargo deny check
cargo vet
osv-scanner scan source -r .
semgrep scan --config p/rust --error .
semgrep scan --config p/security-audit --error .
gitleaks git --no-banner --redact .
gitleaks dir --no-banner --redact <temporary source snapshot excluding .git and target>
codeql database analyze target/quality/codeql-db-rust codeql/rust-queries --format=sarif-latest
cargo semver-checks --workspace --baseline-rev HEAD
rust-code-analysis-cli -m -p crates -O json -o target/quality/reports/rust-code-analysis-local-sql
jscpd . --reporters json,console --output target/quality/reports/jscpd-local-sql --ignore "**/target/**,**/.git/**,**/reports/**"
rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|Unpin|Send|Sync" crates
cargo geiger --manifest-path /Users/alexanderbut/code_projects/personal/cdf/crates/cdf-cli/Cargo.toml --all-targets --all-features --include-tests --locked
cargo bloat --release -p cdf-cli --bin cdf-cli -n 20
```

Results:

- Formatting passed.
- Whitespace diff check passed.
- Workspace `cargo check` passed for default, all-features, and no-default-features.
- Workspace `cargo clippy` passed for default, all-features, and no-default-features.
- `cargo test -p cdf-cli --locked --no-fail-fast` passed: 12 tests passed, 0 failed, including 4 system-SQL tests.
- `cargo test --workspace --all-targets --locked --no-fail-fast` passed.
- `cargo test --workspace --all-targets --all-features --locked --no-fail-fast` passed.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast` passed.
- `cargo doc --workspace --all-features --no-deps --locked` passed.
- `cargo nextest run --workspace --locked` passed: 129 tests passed, 0 skipped.
- `cargo hack check --workspace --all-targets --each-feature --locked` passed.
- `cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings` passed.
- `cargo llvm-cov --workspace --all-features --locked --summary-only` passed and reported 71.99% line coverage, 71.76% function coverage, and 75.54% region coverage across the workspace. `crates/cdf-cli/src/system_sql.rs` reported 69.27% line coverage and 67.50% function coverage.
- `cargo machete` passed with no unused dependency candidates.
- `cargo audit` passed with 0 vulnerabilities. `cargo deny check advisories` passed. OSV reported 0 results.
- Semgrep Rust and security-audit scans passed with 0 findings.
- `gitleaks git` and a source-snapshot `gitleaks dir` scan excluding `.git` and generated `target/` output passed with 0 findings.
- Reused CodeQL database analysis at `target/quality/codeql-db-rust` passed with 0 SARIF results. The database was not recreated for this slice.
- `cargo semver-checks --workspace --baseline-rev HEAD` passed with no semver update required.
- `rust-code-analysis-cli` completed and wrote metrics under `target/quality/reports/rust-code-analysis-local-sql`.
- `jscpd` completed: 117 total clones, 3.02% duplicated lines, and 0 new clones.
- Direct source unsafe search found no first-party unsafe blocks, unsafe impls, unsafe traits, FFI, raw-pointer conversions, `transmute`, or `MaybeUninit`; matches were existing `Send`/`Sync` bounds and the phrase "unsafe unit" in retry messaging.
- `cargo geiger` for `cdf-cli` with tests included reported `cdf-cli 0.1.0` as `0/0` unsafe, then exited nonzero because dependency crates contain unsafe warnings. The added direct `rusqlite` dependency is already present through `cdf-state-sqlite`; geiger reports unsafe in `rusqlite` and native dependencies, not in this CLI code.
- `cargo bloat --release -p cdf-cli --bin cdf-cli -n 20` passed. The release binary text section is 28.7 MiB and top symbols are dominated by DuckDB/libduckdb, not the local SQL module.

Repository-level policy/tooling limits:

- Full `cargo deny check` exited 4 because the repository still has no ratified `deny.toml` license allowlist; this rejects normal present licenses such as Apache-2.0 and MIT. The blocker is owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.
- `cargo vet` exited 255 because `supply-chain/` has not been initialized. The adoption decision is owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.
- A raw whole-repository `gitleaks dir .` scan found generated `target/**` build artifacts from bundled third-party DuckDB/mbedTLS/Parquet sources. Source-snapshot and git-history scans were clean, so no tracked-source secret was found.
- CodeQL analysis reused the existing database as requested. The extractor-coverage issue was later closed by `.10x/tickets/done/2026-07-06-improve-codeql-rust-extractor-coverage.md`.

System-SQL tests cover:

- Joined query over package, segment, checkpoint, and receipt tables.
- Stable JSON columns and rows.
- Concise human output.
- Empty local history query does not create the state DB or package root.
- Mutating SQL rejection does not create state DB or package root.

## What this supports or challenges

This supports closing `.10x/tickets/done/2026-07-06-local-system-sql.md` and removing `sql` from the current CLI blocker list. It proves a practical local system-history SQL surface without inventing runtime commit or destination mirror semantics.

This does not close the full observability parent ticket: destination mirror drift, `inspect run`, tracing fields, and freshness SLO evaluation still need their own executable slices.

## Limits

`cdf sql` currently mounts local checkpoint and package metadata only. It does not query live DuckDB/Postgres mirrors, destination drift probes, package trace text, Parquet stats/quarantine/lineage, or remote/object-store package roots. The SQL surface is intentionally read-only and not a general SQLite shell.

This evidence does not ratify supply-chain license policy, cargo-vet adoption, or CodeQL extractor coverage limits; those remain separate active tickets.
