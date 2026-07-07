Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-06-native-parquet-writer-archive.md, .10x/decisions/native-arrow-datafusion-parquet-policy.md, .10x/specs/package-lifecycle-determinism.md, .10x/specs/destination-receipts-guarantees.md

# Native Parquet writer and archive evidence

## What was observed

The package archive transcode and Parquet destination materialization path now use native arrow-rs Parquet writer APIs rather than DuckDB staging/export. `cdf-package` owns the shared `ArrowWriter` path, and `cdf-dest-parquet` delegates segment materialization to that shared writer.

The symlink requested by the user also exists: `/Users/alexanderbut/code_projects/personal/cdf` is a symlink resolving to `/Users/alexanderbut/code_projects/personal/firn`.

## Procedure

Implementation inspection:

- `crates/cdf-package/src/parquet.rs` writes record batches through `parquet::arrow::ArrowWriter`, validates shared schema, rejects duplicate column names, and rejects unsupported Arrow types before writing.
- `crates/cdf-dest-parquet/src/package.rs` calls `cdf_package::transcode_record_batches_to_parquet_bytes`.
- `crates/cdf-dest-parquet/src/duckdb_writer.rs` was removed.
- `crates/cdf-package/Cargo.toml` and `crates/cdf-dest-parquet/Cargo.toml` no longer depend on `duckdb` for this writer/archive surface; `cdf-package` depends on `parquet 59.0.0`, and `cdf-dest-parquet` uses `parquet 59.0.0` only for tests.

Commands observed:

- `test -L /Users/alexanderbut/code_projects/personal/cdf && readlink /Users/alexanderbut/code_projects/personal/cdf && test "$(readlink /Users/alexanderbut/code_projects/personal/cdf)" = "/Users/alexanderbut/code_projects/personal/firn"`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo test -p cdf-package -p cdf-dest-parquet --locked --no-fail-fast`: passed; 15 `cdf-dest-parquet` tests, 26 `cdf-package` tests, and doc tests passed.
- `cargo clippy -p cdf-package -p cdf-dest-parquet --all-targets --locked -- -D warnings`: passed in the worker run.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo check --workspace --all-targets --all-features --locked`: passed.
- `cargo check --workspace --all-targets --no-default-features --locked`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings`: passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast`: passed.
- `cargo test --workspace --all-targets --all-features --locked --no-fail-fast`: passed.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed.
- `cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo nextest run -p cdf-package -p cdf-dest-parquet --locked`: passed; 41 tests passed.
- `cargo hack check --workspace --all-targets --each-feature --locked`: passed.
- `cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings`: passed.
- `cargo metadata --format-version=1 --locked`, `cargo tree --workspace --locked`, and `cargo tree --workspace --locked -d`: passed; reports are under `target/quality/reports/native-parquet-writer-archive/`.
- `cargo deny check`: passed; final line reports `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo audit --json`: exited 0; JSON reports no vulnerabilities and one warning for ratified `RUSTSEC-2024-0436` on `paste 1.0.15`.
- `osv-scanner scan source -r . --format json --output target/quality/reports/native-parquet-writer-archive/osv.json`: exited 1 because it reports ratified `RUSTSEC-2024-0436` on `paste 1.0.15`; no other OSV package finding was present.
- `cargo vet --locked --output-format json --output-file target/quality/reports/native-parquet-writer-archive/cargo-vet.json`: passed with conclusion `success`.
- `cargo machete`: passed; no unused dependency candidates.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/native-parquet-writer-archive/semgrep-rust.json crates/cdf-package crates/cdf-dest-parquet`: passed with 0 findings.
- `semgrep scan --config p/security-audit --error --json --output target/quality/reports/native-parquet-writer-archive/semgrep-security.json crates/cdf-package crates/cdf-dest-parquet`: passed with 0 findings.
- `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/native-parquet-writer-archive/gitleaks-source.json target/quality/source-snap-native-parquet-writer-archive`: passed with no leaks.
- `rg -n "duckdb::Connection|duckdb-arrow|duckdb_arrow|DuckDB Parquet|read_parquet\(|COPY \(SELECT|Arrow 58|duckdb_writer" crates/cdf-package crates/cdf-dest-parquet`: no matches.
- `rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|\*const|\*mut" crates/cdf-package crates/cdf-dest-parquet`: no matches.
- Scoped dependency tree reports under `target/quality/reports/native-parquet-writer-archive/cdf-package-tree.txt` and `target/quality/reports/native-parquet-writer-archive/cdf-dest-parquet-tree.txt` show `parquet v59.0.0` in the scoped writer path and no `duckdb` line.
- `cargo tree --workspace --locked -i paste`: shows `paste v1.0.15` through `parquet v59.0.0`, matching `.10x/decisions/native-arrow-datafusion-parquet-policy.md`.
- `cargo mutants -p cdf-package --file crates/cdf-package/src/parquet.rs --re 'transcode_record_batches_to_parquet_bytes|validate_fields|validate_parquet_type' --test-tool cargo --jobs 4 --timeout 300 --baseline skip --cargo-arg=--locked --output target/quality/mutants-native-parquet-writer-archive`: passed; 6 mutants tested, 6 caught.

Tool limitations and explicit skips:

- CodeQL was skipped for this slice because the active goal explicitly says to skip CodeQL for now and the user specifically requested not recreating the CodeQL database. The existing reusable database remains under `target/quality/codeql-db-rust`.
- `cargo geiger` was attempted for the scoped crates. Workspace `-p` mode failed against the virtual manifest; absolute-manifest full scans and `--forbid-only` mode were killed after hanging without output. The limitation is recorded in `target/quality/reports/native-parquet-writer-archive/cargo-geiger-limitation.txt`. Source-level unsafe scans over the touched crates found no unsafe, FFI, raw pointer, or transmute patterns.
- Coverage, Miri, cargo-careful, fuzzing, Kani, benchmarks, profiling, and binary-size checks were not run because this change removes a DuckDB staging writer in safe Rust, does not add unsafe code or new parser surfaces, and is already covered by direct package/destination tests plus bounded mutation.

## What this supports or challenges

This supports the acceptance criteria that package archive transcode and Parquet destination materialization no longer use DuckDB-backed export, while preserving Arrow IPC package identity, archive sidecars, fidelity reports, object manifests, replace pointers, receipt verification, duplicate/idempotent behavior, and destination conformance.

This also supports the native Arrow/DataFusion Parquet policy: the advisory scanners distinguish the ratified `RUSTSEC-2024-0436` `paste` path from any other advisory finding in this slice.

## Limits

The evidence does not remove DuckDB from the dedicated DuckDB destination or unrelated workspace consumers. It does not close broader native Arrow/DataFusion migration work outside the package archive writer and Parquet destination writer paths. CodeQL and geiger are not pass evidence for this slice because they were skipped or limited as described above.
