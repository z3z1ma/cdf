Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-duckdb-ledger-mirror-doctor-drift.md, .10x/specs/checkpoint-state-cdf-line.md, .10x/specs/project-cli-observability-security.md, .10x/specs/destination-receipts-guarantees.md

# DuckDB Ledger Mirror Doctor Drift Evidence

## What was observed

`cdf doctor` now replaces the prior unsupported `ledger_destination_drift` check for local `duckdb://` destinations with a concrete read-only probe when the SQLite state database and DuckDB destination database both already exist.

The probe:

- Checks for the SQLite and DuckDB files before opening either database for drift inspection.
- Opens SQLite with `SQLITE_OPEN_READ_ONLY`.
- Opens DuckDB with `AccessMode::ReadOnly` through `DuckDbDestination::read_mirror_snapshot_read_only`.
- Does not create missing SQLite databases, DuckDB databases, or mirror tables.
- Reads only existing `_cdf_loads` and `_cdf_state` tables.
- Compares committed local ledger heads with receipts against `_cdf_loads` by `(target, idempotency_token)` and `_cdf_state` by `(target, package_hash, segment_id)`.
- Reports missing, mismatched, and unreconciled extra load/state mirror rows as `failed`.
- Emits structured JSON details with counts and capped examples, without printing raw receipt JSON.

New CLI tests cover:

- Skipped drift check when local databases are missing, including assertions that missing SQLite and DuckDB files are not created.
- Clean real DuckDB commit plus SQLite checkpoint ledger reports `passed`.
- State mirror mismatch reports `failed` and exits nonzero.
- Divergent receipt target reports missing and extra load/state rows and exits nonzero.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/cdf`:

```text
cargo test -p cdf-dest-duckdb --locked --no-fail-fast
cargo test -p cdf-cli --locked --no-fail-fast
cargo fmt --all -- --check
cargo check -p cdf-dest-duckdb -p cdf-cli --locked
cargo clippy -p cdf-dest-duckdb -p cdf-cli --all-targets --locked -- -D warnings
git diff --check -- . ':(exclude).gitignore'
cargo check --workspace --all-targets --locked
cargo check --workspace --all-targets --all-features --locked
cargo check --workspace --all-targets --no-default-features --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings
cargo test --workspace --all-targets --locked --no-fail-fast
cargo test --workspace --doc --all-features --locked --no-fail-fast
cargo nextest run --workspace --locked
cargo hack check --workspace --all-targets --each-feature --locked
cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings
cargo semver-checks --workspace --baseline-rev HEAD
cargo llvm-cov --workspace --all-features --locked --summary-only
cargo doc --workspace --all-features --no-deps --locked
cargo machete
cargo audit --json > target/quality/reports/cargo-audit-duckdb-drift.json
cargo deny check advisories
cargo deny check
cargo vet
osv-scanner scan source -r .
tools/codeql-rust-quality.sh
semgrep scan --config p/rust --error .
semgrep scan --config p/security-audit --error .
semgrep scan --config p/rust --error crates/cdf-cli/src/doctor_drift.rs crates/cdf-dest-duckdb/src/api.rs crates/cdf-dest-duckdb/src/mirrors.rs
semgrep scan --config p/security-audit --error crates/cdf-cli/src/doctor_drift.rs crates/cdf-dest-duckdb/src/api.rs crates/cdf-dest-duckdb/src/mirrors.rs
gitleaks git --no-banner --redact .
gitleaks dir --no-banner --redact <temporary source snapshot from git tracked plus untracked source files>
rust-code-analysis-cli -m -p crates -O json -o target/quality/reports/rust-code-analysis-duckdb-drift
jscpd . --reporters json,console --output target/quality/reports/jscpd-duckdb-drift --ignore "**/target/**,**/.git/**,**/reports/**"
rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|Unpin|Send|Sync" crates
CARGO_TARGET_DIR=target/quality/geiger-target cargo geiger --manifest-path /Users/alexanderbut/code_projects/personal/cdf/crates/cdf-cli/Cargo.toml --all-targets --all-features --include-tests --locked
cargo bloat --release -p cdf-cli --bin cdf-cli -n 20
```

Results:

- Parent integration reran `cargo test -p cdf-dest-duckdb --locked --no-fail-fast`, `cargo test -p cdf-cli --locked --no-fail-fast`, and `cargo fmt --all -- --check`; all exited 0 before broader quality checks.
- `cargo test -p cdf-dest-duckdb --locked --no-fail-fast` exited 0: 8 tests passed, 0 failed, 0 ignored; doc-tests 0 passed.
- `cargo test -p cdf-cli --locked --no-fail-fast` exited 0 after adding the final drift fixture: 16 tests passed, 0 failed, 0 ignored; binary tests 0 passed; doc-tests 0 passed.
- `cargo fmt --all -- --check` exited 0.
- `cargo check -p cdf-dest-duckdb -p cdf-cli --locked` exited 0.
- `cargo clippy -p cdf-dest-duckdb -p cdf-cli --all-targets --locked -- -D warnings` exited 0.
- `git diff --check -- . ':(exclude).gitignore'` exited 0.
- Workspace `cargo check` exited 0 for default, all-features, and no-default-features.
- Workspace `cargo clippy` exited 0 for default, all-features, and no-default-features.
- `cargo test --workspace --all-targets --locked --no-fail-fast` exited 0. It ran 133 tests across workspace unit binaries, with 133 passed and 0 failed.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast` exited 0.
- `cargo nextest run --workspace --locked` exited 0: 133 tests passed, 0 skipped.
- `cargo hack check --workspace --all-targets --each-feature --locked` exited 0.
- `cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings` exited 0.
- `cargo semver-checks --workspace --baseline-rev HEAD` exited 0 with no semver update required.
- `cargo llvm-cov --workspace --all-features --locked --summary-only` exited 0 and reported 73.30% region coverage, 72.60% function coverage, and 76.96% line coverage. `crates/cdf-cli/src/doctor_drift.rs` reported 85.82% region coverage, 82.35% function coverage, and 88.95% line coverage.
- `cargo doc --workspace --all-features --no-deps --locked` exited 0.
- `cargo machete` exited 0 with no unused dependency candidates.
- `cargo audit --json` exited 0 and reported 0 vulnerabilities and 0 warnings.
- `cargo deny check advisories` exited 0. Full `cargo deny check` exited 4 on the existing unratified license allowlist policy, with advisories/bans/sources OK and licenses failed; this remains owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.
- `cargo vet` exited 255 because `supply-chain/` has not been initialized; this remains owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.
- `osv-scanner scan source -r .` exited 0 with no issues.
- `tools/codeql-rust-quality.sh` refreshed the reusable database at `target/quality/codeql-db-rust` because the input fingerprint was missing after the new wrapper landed. The run wrote `target/quality/codeql-db-rust/cdf-codeql-inputs.sha256`, analyzed successfully, produced 0 SARIF findings, and reported 115 Rust files extracted, 82 with warnings, 33 without warnings, 1186 extraction warnings, and 0 extraction errors. These warnings match the recorded local CodeQL Rust extractor macro limit.
- Semgrep Rust and security-audit scans exited 0 with 0 findings. Additional targeted Semgrep scans over the new untracked `doctor_drift.rs` and changed DuckDB files also exited 0 with 0 findings.
- `gitleaks git` and a temporary source snapshot scan from tracked plus untracked source files exited 0 with no leaks.
- `rust-code-analysis-cli` exited 0 and wrote JSON metrics under `target/quality/reports/rust-code-analysis-duckdb-drift`.
- `jscpd` exited 0. It reported 126 total clones and 3.14% duplicated lines. Rust-specific duplication was 40 clones and 1.48% duplicated lines. The new drift module contributes a few small helper-shape clones; no abstraction was added because the repeated structures keep the drift examples explicit and localized.
- Direct first-party source search found no `unsafe` blocks, `unsafe impl`, `unsafe trait`, FFI, raw pointer conversions, `transmute`, or `MaybeUninit`. Matches were existing `Send`/`Sync` bounds and the word "unsafe" in retry prose.
- `cargo geiger` was run with isolated `CARGO_TARGET_DIR=target/quality/geiger-target` and exited 1 on dependency parse/scan warnings and dependency unsafe usage. This matches prior geiger limits; the paired direct source search found no first-party unsafe introduced by this slice.
- `cargo bloat --release -p cdf-cli --bin cdf-cli -n 20` exited 0. The release binary text section is 28.9 MiB, with top symbols dominated by DuckDB/libduckdb. `cdf_cli::commands::dispatch` is 43.5 KiB; the drift module does not appear in the top 20 symbols.

## What this supports or challenges

This supports closing `.10x/tickets/done/2026-07-06-duckdb-ledger-mirror-doctor-drift.md`. The implementation satisfies the DuckDB-local drift slice without adding Postgres drift behavior, recovery, checkpoint mutation, mirror repair, or mirror table creation.

The no-creation requirement is supported directly by the skipped CLI test, which checks both missing state and destination paths remain absent after `cdf doctor`.

## Limits

The tests use one segment per package. The comparison code handles multiple ledger heads and multiple segments by map keys, but no multi-segment fixture was added in this slice.

The probe treats non-DuckDB destinations as unsupported and does not implement Postgres live drift inspection.

If an existing DuckDB database lacks mirror tables and local committed heads exist, the absent table yields missing mirror rows instead of table creation. If no committed heads exist, absent mirror tables produce zero mirror rows and no drift.

Full supply-chain policy closure is still blocked by unratified `cargo deny` license policy and uninitialized `cargo vet` metadata. Those are repository-level policy issues already owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`, not new drift-check regressions.
