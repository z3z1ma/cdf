Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-06-native-parquet-file-source.md, .10x/decisions/native-arrow-datafusion-parquet-policy.md

# Native Parquet file source evidence

## What was observed

`cdf-formats` now reads Parquet file sources through the native arrow-rs `parquet 59.0.0` reader path instead of DuckDB `read_parquet(?)` plus an Arrow 58 IPC bridge.

The dependency graph shows `parquet v59.0.0 -> cdf-formats` and `paste v1.0.15 -> parquet v59.0.0 -> cdf-formats`. `cdf-formats` no longer has a direct normal dependency on `duckdb`, and the changed source has no `duckdb::Connection`, `duckdb-arrow`, `duckdb_arrow`, `DuckDB Parquet`, `read_parquet(?)`, or `Arrow 58` references.

## Procedure

Implementation checks:

- `cargo fmt --all -- --check` passed.
- `git diff --check` passed.
- `cargo metadata --locked --format-version 1` passed.
- `cargo check --workspace --all-targets --locked` passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` passed.
- `cargo test -p cdf-formats --locked --no-fail-fast` passed: 6 tests.
- `cargo nextest run -p cdf-formats --locked` passed: 6 tests.
- `cargo test --workspace --locked --no-fail-fast` passed: 301 unit/integration tests plus workspace doc-test crates.
- `cargo doc -p cdf-formats --no-deps --locked` passed.
- `cargo machete --with-metadata` passed with no unused dependencies.

Dependency and advisory checks:

- `cargo tree -p cdf-formats --locked --edges normal -i duckdb` printed nothing to report.
- `cargo tree --workspace --locked -i parquet` showed `parquet v59.0.0 -> cdf-formats`.
- `cargo tree --workspace --locked -i paste` showed `paste v1.0.15 -> parquet v59.0.0 -> cdf-formats`.
- `cargo vet --locked --output-format json --output-file target/quality/reports/cargo-vet-native-parquet-file-source.json` passed with `conclusion: success` and `failures: 0` after `cargo vet fmt` normalized current-version exemptions for the newly introduced Parquet transitive crates.
- `cargo deny check > target/quality/reports/deny-native-parquet-file-source.txt 2>&1` passed: advisories, bans, licenses, and sources all ok.
- `cargo audit --json > target/quality/reports/cargo-audit-native-parquet-file-source.json` reported `vulnerability_count: 0`; it reported the informational unmaintained advisory `RUSTSEC-2024-0436` for `paste 1.0.15`.
- `osv-scanner scan source -r . --format json --output target/quality/reports/osv-native-parquet-file-source.json` reported one finding, `RUSTSEC-2024-0436`, matching the ratified native Parquet policy exception.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-native-parquet-file-source.json crates/cdf-formats` passed with 0 findings and 0 errors.
- `semgrep scan --config p/security-audit --error --json --output target/quality/reports/semgrep-security-native-parquet-file-source.json crates/cdf-formats` passed with 0 findings and 0 errors.
- `gitleaks detect --source target/quality/source-snap-native-parquet --no-git --report-format json --report-path target/quality/reports/gitleaks-native-parquet-file-source.json` passed with 0 leaks against a tracked-file source snapshot.

Soundness and mutation checks:

- `rg -n "\bunsafe\b|extern \"|\*const|\*mut|unsafe impl|impl (Send|Sync)" crates/cdf-formats` found no matches.
- `cargo mutants -p cdf-formats --file crates/cdf-formats/src/readers.rs --re 'read_parquet_file_with_scope|parquet_data_error' --test-tool cargo --jobs 4 --timeout 300 --baseline skip --cargo-arg=--locked --output target/quality/mutants-native-parquet-file-source` produced 2 mutants, both unviable, with 0 missed and 0 timeouts. The unviable mutations attempted to replace `read_parquet_file_with_scope` and `parquet_data_error` with `Default::default()`.
- `cargo geiger --manifest-path /Users/alexanderbut/code_projects/personal/firn/crates/cdf-formats/Cargo.toml --locked` was attempted, but did not produce a usable final report after noisy dependency metadata output. The direct changed-crate unsafe scan above is the recorded unsafe evidence for this slice.

CodeQL was skipped per the active goal instruction to skip CodeQL for now and avoid recreating the CodeQL database.

## What this supports or challenges

This supports closing `.10x/tickets/done/2026-07-06-native-parquet-file-source.md`: Parquet file sources now use the native arrow-rs Parquet reader, tests cover deterministic descriptor/batch behavior, schema hash propagation, source positions, malformed Parquet errors, and package replay compatibility, and the only advisory introduced is the previously ratified `RUSTSEC-2024-0436` path through `paste`.

The evidence challenges no active specification or decision. It replaces the older DuckDB-backed workaround from `.10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md` only for file-source reads; package archive writer replacement is separately owned by `.10x/tickets/done/2026-07-06-native-parquet-writer-archive.md`.

## Limits

This does not prove Parquet destination/archive writer native replacement, because that is explicitly out of scope. OSV still reports `RUSTSEC-2024-0436`, and `cargo audit` still reports the corresponding unmaintained warning; both are accepted only under `.10x/decisions/native-arrow-datafusion-parquet-policy.md` and the scoped `deny.toml` exception. Mutation testing did not yield caught mutants because the generated mutants were unviable.
