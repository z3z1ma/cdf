Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-source-decode-type-drift-quarantine-seam.md, .10x/tickets/done/2026-07-08-p1-e6-drift-quarantine-conformance.md, .10x/decisions/source-decode-type-drift-quarantine.md

# Source decode type-drift quarantine seam evidence

## What was observed

The source-decode type-drift seam is implemented end to end:

- `cdf-kernel::BatchHeader` carries optional `pre_contract_quarantine` facts with serde defaults for legacy headers.
- `cdf-formats` declared-schema JSON/NDJSON reads now localize scalar type mismatches into `PreContractQuarantineFact` values, omit only offending rows, preserve accepted-row order, fail closed for malformed JSON and unlocalizable row-shape errors, and apply `pii:*` redaction to observed source values.
- `cdf-declarative` passes declared schemas into file runtime JSON/NDJSON reads.
- `cdf-engine` folds pre-contract facts into package quarantine artifacts and verdict/quarantine summaries before accepted rows continue through normal contract execution.
- The E6 drift-quarantine conformance fixture now uses literal numeric JSON `event_type: 42` under a frozen string contract and asserts `rule_id = "source-decode:event_type:type-mismatch"` and `error_code = "source_type_mismatch"` in package and Postgres mirror evidence.
- CLI preview fixtures with declared `updated_at: int64` were repaired to use numeric values so the new declared-schema reader does not correctly quarantine those test rows.
- Live-run golden fixtures changed because declared-schema JSON/NDJSON reads now preserve declared Arrow schema metadata in package identity; the new hashes were verified by the 100-run DuckDB/Parquet and bounded Postgres golden tests.

## Procedure

Focused behavior checks:

- `cargo test --locked -p cdf-formats declared_ndjson -- --nocapture` passed: 3 tests.
- `cargo test --locked -p cdf-engine source_decode_quarantine_facts_fold_into_package_artifacts -- --nocapture` passed: 1 test.
- `cargo test --locked -p cdf-kernel batch_header_serde_defaults_missing_pre_contract_quarantine -- --nocapture` passed: 1 test.
- `cargo test --locked -p cdf-conformance drift_quarantine -- --nocapture` passed: 2 tests.
- `cargo test --locked -p cdf-cli preview_ -- --nocapture` passed: 13 tests.
- `cargo test --locked -p cdf-conformance live_local_file_ -- --nocapture` passed: 4 tests, including DuckDB and Parquet 100-run golden loops and bounded Postgres golden coverage.

Full correctness gates after repairs:

- `cargo fmt --all -- --check` passed.
- `git diff --check` passed.
- `cargo check --workspace --all-targets --locked` passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` passed.
- `cargo check --workspace --all-targets --all-features --locked` passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` passed.
- `cargo check --workspace --all-targets --no-default-features --locked` passed.
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings` passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast` passed.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast` passed.
- `cargo doc --workspace --all-features --no-deps --locked` passed; report: `target/quality/reports/cargo-doc-source-decode-seam.txt`.

Quality, security, and metrics:

- `jscpd` over touched Rust files completed with 13 sources, 11,562 lines, 118 clones, 1,165 duplicated lines, 10.08% duplicated lines, and 0 new clones; report: `target/quality/reports/jscpd-source-decode-seam/jscpd-report.json`.
- `rust-code-analysis-cli -m -O json -p crates` completed; report: `target/quality/reports/rust-code-analysis-source-decode-seam.json`. Maximum observed workspace metrics in this report were cyclomatic 438 in `crates/cdf-dest-postgres/src/source.rs` and cognitive 142 in `crates/cdf-declarative/src/rest_runtime.rs`; these are existing hotspots outside the changed seam.
- `scc --format json` over touched files completed; report: `target/quality/reports/scc-source-decode-seam.json`.
- Direct unsafe/FFI/raw-pointer scan over touched files found 0 lines; report: `target/quality/reports/unsafe-rg-source-decode-seam.txt`.
- `semgrep scan --config p/rust` over touched files passed with 0 findings; report: `target/quality/reports/semgrep-rust-source-decode-seam.json`.
- `gitleaks detect --no-git --source crates --redact` passed with 0 leaks; report: `target/quality/reports/gitleaks-crates-source-decode-seam.json`.
- `cargo audit --json` reported 0 vulnerabilities and one unmaintained warning for the ratified `paste 1.0.15` advisory `RUSTSEC-2024-0436`; report: `target/quality/reports/cargo-audit-source-decode-seam.json`.
- `cargo deny check` exited 0 with advisories, bans, licenses, and sources ok; it still warns about the ratified Arrow 58/59 duplicate tuple; report: `target/quality/reports/cargo-deny-source-decode-seam.txt`.
- `cargo vet --locked` passed: `Vetting Succeeded (424 exempted)`; report: `target/quality/reports/cargo-vet-source-decode-seam.txt`.
- `osv-scanner scan source -r .` exited nonzero only for the ratified `paste 1.0.15` / `RUSTSEC-2024-0436` advisory; report: `target/quality/reports/osv-source-decode-seam.json`.
- `cargo geiger` with isolated target dir over `cdf-formats` completed; first-party packages in that dependency scan (`cdf-contract`, `cdf-formats`, `cdf-kernel`) used 0 unsafe items. Geiger emitted dependency matcher warnings; report: `target/quality/reports/cargo-geiger-cdf-formats-source-decode-seam.json`.
- `tools/codeql-rust-quality.sh` used the reusable database path `target/quality/codeql-db-rust`. It refreshed the database because the source/manifest fingerprint changed, analyzed 227 Rust files, produced 0 SARIF results, 0 extraction errors, and 3,484 extraction warnings consistent with the known Rust extractor macro limitation in `.10x/knowledge/quality-gate-execution.md`; report: `target/quality/reports/codeql-source-decode-seam.log`.
- `cargo machete --with-metadata` found no unused dependencies in the touched implementation crates and reported the pre-existing `cdf-cli -> cdf-dest-parquet` hint. That hint is already owned by `.10x/tickets/2026-07-08-cdf-cli-unused-parquet-dependency.md`; report: `target/quality/reports/cargo-machete-source-decode-seam.txt`.

## What this supports

This supports closing the source-decode seam and E6 drift-quarantine conformance: literal source scalar type drift is now quarantined, accepted rows continue, package quarantine evidence and summaries are written, destination mirror behavior is asserted, trait receipt verification and checkpoint gating remain covered by the conformance path, and redaction of pre-contract observed values is tested.

It also supports the golden fixture changes: the only intended golden churn is package identity evidence affected by declared-schema Arrow metadata; row counts, source hash, source size, destination rows, and destination mirror counts remain stable.

## Limits

This does not implement a broad schema-on-read replacement, DataFusion multi-output plan, silent scalar coercion, new destination, new source archetype, or public demo script.

`cargo machete` still reports the existing `cdf-cli` direct `cdf-dest-parquet` hint, owned separately. `osv-scanner` and cargo-audit still surface only the ratified `paste` maintenance advisory. CodeQL extraction warnings remain the known local extractor limitation and have 0 SARIF results.
