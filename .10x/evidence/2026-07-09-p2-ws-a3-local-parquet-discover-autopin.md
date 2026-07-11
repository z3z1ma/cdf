Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-a3-local-parquet-discover-autopin.md, .10x/tickets/done/2026-07-08-p2-ws-a-discovery-compiler-stage.md, .10x/tickets/done/2026-07-08-p2-data-onramp-program.md

# P2 WS-A3 local Parquet discover CLI and auto-pin evidence

## What was observed

The A3 slice added an operator-visible `cdf schema discover <resource>` command for local single-file Parquet discover-mode resources and auto-pins the same local Parquet footer discovery before `cdf plan` and `cdf run`.

The discovered Parquet footer schema is normalized through `namecase-v1`, preserves physical names in `cdf:source_name`, writes deterministic schema snapshots only on plan/run auto-pin, and feeds plan/run through a cloned `SchemaSource::Discovered` resource. The non-mutating `schema discover` command reports the candidate snapshot path/hash and source identity without writing `.cdf/schemas`, `cdf.lock`, package, destination, or checkpoint artifacts.

## Procedure

Focused behavior checks:

- `cargo test -p cdf-project local_parquet_discover --locked`
- `cargo test -p cdf-cli schema_discover --locked`
- `cargo test -p cdf-cli parquet_discover --locked`
- `cargo test -p cdf-cli unsupported_discover --locked`
- `cargo test -p cdf-cli preview_succeeds_for_csv_json_parquet_and_arrow_ipc_file_resources --locked`
- `cargo test -p cdf-project -p cdf-cli --locked`

Quality gates:

- `cargo fmt --all -- --check`
- `cargo clippy -p cdf-project -p cdf-cli --all-targets --locked -- -D warnings`
- `git diff --check`
- `jscpd --min-lines 12 --min-tokens 80 --reporters console,json --output target/quality/reports/jscpd-a3-impl --format rust --no-colors --exit-code 1 <touched implementation files>`
- `jscpd --min-lines 12 --min-tokens 80 --reporters console,json --output target/quality/reports/jscpd-a3 --format rust --no-colors --exit-code 1 <touched Rust files including tests>`
- `rust-code-analysis-cli -m -O json <touched Rust files> > target/quality/reports/rust-code-analysis-a3.json`
- `semgrep scan --config p/rust --error --metrics=off --json --output target/quality/reports/semgrep-a3.json <touched Rust files>`
- `gitleaks detect --no-git --source crates/cdf-project/src --report-format json --report-path target/quality/reports/gitleaks-a3-cdf-project.json --no-banner --redact`
- `gitleaks detect --no-git --source crates/cdf-cli/src --report-format json --report-path target/quality/reports/gitleaks-a3-cdf-cli.json --no-banner --redact`
- `gitleaks detect --no-git --source crates/cdf-declarative/src --report-format json --report-path target/quality/reports/gitleaks-a3-cdf-declarative.json --no-banner --redact`
- `cargo deny check > target/quality/reports/cargo-deny-a3.txt 2>&1`
- `cargo audit --json > target/quality/reports/cargo-audit-a3.json`
- `cargo vet --locked > target/quality/reports/cargo-vet-a3.txt 2>&1`
- `cargo machete > target/quality/reports/cargo-machete-a3.txt 2>&1`
- `osv-scanner scan --lockfile Cargo.lock --format json --output target/quality/reports/osv-a3.json`
- `tools/codeql-rust-quality.sh > target/quality/reports/codeql-rust-a3.log 2>&1`

## Results

- Focused `cdf-project` discovery tests passed: 5 passed, 0 failed.
- Focused `cdf-cli` discover command test passed: 1 passed, 0 failed.
- Focused `cdf-cli` Parquet discover plan/run tests passed: 3 passed, 0 failed.
- Focused unsupported discover test passed: 1 passed, 0 failed.
- Focused preview fixture regression test passed: 1 passed, 0 failed.
- Final broad `cargo test -p cdf-project -p cdf-cli --locked` passed: `cdf-cli` 203 unit tests plus 1 `doctor_env` integration test; `cdf-project` 94 unit tests; both doctest sets empty/pass.
- Final `cargo fmt --all -- --check`, clippy, and `git diff --check` passed.
- Implementation-only `jscpd` passed: 12 files, 5,561 lines, 0 clones, 0 duplicated lines.
- Broad touched-file `jscpd` reported 15 clones, 239 duplicated lines, 1.55%. The remaining clone ranges are pre-existing `crates/cdf-cli/src/tests.rs` preview/resume scaffold patterns; the A3-introduced Parquet fixture duplicates were removed before final verification. Prior no-action rationale for the large CLI-test duplication surface exists in `.10x/reviews/2026-07-07-cli-sql-run-success-review.md` and `.10x/reviews/2026-07-08-cli-status-runtime-ledger-freshness-review.md`.
- `rust-code-analysis-cli` completed over 14 touched Rust files; report captured in `target/quality/reports/rust-code-analysis-a3.json`. The new `schema_discovery.rs` report showed cyclomatic max 10 and cognitive max 3.
- Semgrep scanned 14 files with 11 Rust rules and reported 0 findings.
- Gitleaks source scans over `cdf-project`, `cdf-cli`, and `cdf-declarative` source trees reported no leaks.
- `cargo deny check`, `cargo audit`, `cargo vet --locked`, and `cargo machete` passed. `cargo vet` reported `Vetting Succeeded (455 exempted)`.
- OSV returned the already-ratified `RUSTSEC-2024-0436` `paste` maintenance advisory. This is the same scoped residual owned by the existing supply-chain records; no Cargo metadata changed in this slice.
- CodeQL ran through reusable `target/quality/codeql-db-rust`. It refreshed because Rust source content changed, then completed analysis with extraction errors 0. SARIF contains 3 `rust/hard-coded-cryptographic-value` findings in existing `crates/cdf-cli/src/tests.rs` backfill fixtures, now at lines 1319, 1409, and 1465 after nearby edits. The existing owner is `.10x/tickets/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.

## What this supports

This supports closing the A3 slice: local single-file Parquet discover-mode resources now have a no-write CLI discovery path and first-use plan/run auto-pin into deterministic normalized schema snapshots. The command and auto-pin behavior preserve the P2 anti-convergence rule: discovery is pinned, not silently perpetual.

## Limits

This does not prove remote ranged discovery, multi-file schema union/variance, SQL/database discovery, REST discovery, Python/WASM discovery, `schema pin/show/diff`, `cdf add`, or preview/run parity for discover-mode resources. Those remain outside A3 and inside later P2 WS-A/WS-D/WS-H/WS-I children.
