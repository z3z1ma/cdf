Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p0-c2-rest-sql-run-matrix.md, .10x/tickets/2026-07-07-p0-workstream-c-spine-conformance-harness.md

# P0 C2 REST and SQL run matrix evidence

## What was observed

`cdf-conformance::run_matrix` now covers all current MVP source archetypes through `cdf_project::run_project`: FILE, deterministic REST fixture, and table-backed Postgres SQL. Each source runs against DuckDB, filesystem Parquet, and Postgres destinations across append, replace, and merge where the destination sheet supports the disposition.

The final focused test executed 24 cells and recorded 3 sheet-backed exclusions:

| Source | Destination | Append | Replace | Merge |
| --- | --- | --- | --- | --- |
| file | duckdb | executed | executed | executed |
| file | parquet_filesystem | executed | executed | excluded: Parquet sheet supports append/replace only |
| file | postgres | executed | executed | executed |
| rest | duckdb | executed | executed | executed |
| rest | parquet_filesystem | executed | executed | excluded: Parquet sheet supports append/replace only |
| rest | postgres | executed | executed | executed |
| sql | duckdb | executed | executed | executed |
| sql | parquet_filesystem | executed | executed | excluded: Parquet sheet supports append/replace only |
| sql | postgres | executed | executed | executed |

The focused output counts were:

- `file`: 8 executed cells, 1 excluded cell.
- `rest`: 8 executed cells, 1 excluded cell.
- `sql`: 8 executed cells, 1 excluded cell.
- Total: 24 executed cells, 3 excluded cells.

Each executed cell asserts plan resource/package/scope honesty, `PackageReader::verify`, destination trait-level `DestinationProtocol::verify`, receipt-before-checkpoint gating through `after_receipt_verified`, committed checkpoint head, source position evidence, package artifact replay identity, and duplicate replay no-op behavior.

REST cells use injected `RecordingTransport` and enter runtime through `ProjectRunSource::rest`; no public network is contacted. SQL cells create local Postgres source tables through the existing local/`TEST_DATABASE_URL` Postgres harness and enter runtime through `ProjectRunSource::sql`. REST and SQL cursor source positions both close at `updated_at=20`. The serialized matrix output asserts that fixture secrets and the Postgres URL are not present.

The C1 single large test module was split before REST/SQL expansion into focused modules:

- `assertions.rs`
- `core.rs`
- `destinations.rs`
- `file_fixture.rs`
- `local_postgres.rs`
- `plan_json.rs`
- `rest_fixture.rs`
- `sql_fixture.rs`
- `test_support.rs`
- `tests.rs`

The largest focused module after the split is `destinations.rs` at 297 lines.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

```text
cargo fmt --all --check
git diff --check
cargo check -p cdf-conformance -p cdf-project -p cdf-declarative --all-targets --locked
cargo clippy -p cdf-conformance -p cdf-project -p cdf-declarative --all-targets --locked -- -D warnings
cargo test -p cdf-conformance run_matrix -- --nocapture | tee target/quality/reports/cargo-test-p0-c2-run-matrix.log
cargo nextest run -p cdf-conformance --locked | tee target/quality/reports/cargo-nextest-p0-c2-cdf-conformance.log
jscpd crates/cdf-conformance/src/run_matrix crates/cdf-conformance/src/lib.rs --reporters json,console --output target/quality/reports/jscpd-p0-c2-run-matrix --ignore "**/target/**,**/.git/**,**/reports/**"
rust-code-analysis-cli -m -O json -p crates/cdf-conformance/src/run_matrix > target/quality/reports/rust-code-analysis-p0-c2-run-matrix.json
scc --format json --output target/quality/reports/scc-p0-c2-run-matrix.json crates/cdf-conformance/src/run_matrix
gitleaks detect --no-git --source crates/cdf-conformance/src/run_matrix --report-format json --report-path target/quality/reports/gitleaks-p0-c2-run-matrix.json
semgrep scan --no-git-ignore --config p/rust --error --json --output target/quality/reports/semgrep-p0-c2-run-matrix.json crates/cdf-conformance/src/run_matrix
cargo deny check advisories > target/quality/reports/cargo-deny-advisories-p0-c2.txt
cargo deny check > target/quality/reports/cargo-deny-check-p0-c2.txt
cargo audit --json > target/quality/reports/cargo-audit-p0-c2.json
cargo vet --locked > target/quality/reports/cargo-vet-p0-c2.txt
cargo tree -p cdf-conformance --locked > target/quality/reports/cargo-tree-p0-c2-cdf-conformance.txt
osv-scanner scan source --lockfile Cargo.lock --format json --output-file target/quality/reports/osv-scanner-p0-c2.json
rg -n "\bunsafe\b" crates/cdf-conformance/src/run_matrix crates/cdf-conformance/Cargo.toml Cargo.lock
```

## Results

- `cargo fmt --all --check`: passed.
- `git diff --check`: passed.
- `cargo check -p cdf-conformance -p cdf-project -p cdf-declarative --all-targets --locked`: passed.
- `cargo clippy -p cdf-conformance -p cdf-project -p cdf-declarative --all-targets --locked -- -D warnings`: passed.
- `cargo test -p cdf-conformance run_matrix -- --nocapture`: passed; final output recorded 24 executed cells and 3 excluded Parquet merge cells.
- `cargo nextest run -p cdf-conformance --locked`: passed; 41 tests run, 41 passed, 0 skipped.
- `jscpd`: completed; 11 Rust sources, 1,897 lines, 2 small clones, 20 duplicated lines (1.05%), 162 duplicated tokens (1.37%). The clones are the parallel REST/SQL fixture resource checks and TOML shape.
- `rust-code-analysis-cli`: completed. Unit metrics: `destinations.rs` 297 SLOC, cyclomatic 71, cognitive 13; `assertions.rs` 288 SLOC, cyclomatic 42, cognitive 6; `core.rs` 231 SLOC, cyclomatic 54, cognitive 9; `local_postgres.rs` 212 SLOC, cyclomatic 62, cognitive 4. Max function cyclomatic was 21 in local Postgres startup; max function cognitive was 4 in `copy_dir_all`.
- `scc`: completed; Rust total for `run_matrix`: 11 files, 1,897 lines, 1,760 code lines, aggregate complexity 69.
- `gitleaks`: passed; no leaks found.
- `semgrep --no-git-ignore`: passed; 11 files scanned, 11 Rust rules, 0 findings.
- `cargo deny check advisories`: passed.
- `cargo deny check`: passed with duplicate-version warnings for the already-ratified Arrow 58/59 residual; final summary `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo audit --json`: passed with 0 vulnerabilities and one already-ratified unmaintained warning for `paste 1.0.15` / `RUSTSEC-2024-0436`.
- `cargo vet --locked`: passed; `Vetting Succeeded (393 exempted)`.
- `cargo tree -p cdf-conformance --locked`: completed and wrote `target/quality/reports/cargo-tree-p0-c2-cdf-conformance.txt`.
- `osv-scanner scan source --lockfile Cargo.lock`: returned exit 1 for exactly one finding, the same already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` unmaintained advisory; no additional OSV findings were present.
- `rg -n "\bunsafe\b" crates/cdf-conformance/src/run_matrix crates/cdf-conformance/Cargo.toml Cargo.lock`: found no unsafe use in the touched Rust sources; the only match was the pre-existing `unsafe-libyaml` package name in `Cargo.lock`.

## What this supports

This supports C2 acceptance: conformance now drives FILE, deterministic REST, and table-backed Postgres SQL source archetypes through the general run spine across DuckDB, filesystem Parquet, and Postgres; sheet exclusions are explicit; REST does not contact public network; SQL uses a local Postgres source setup; and executed REST/SQL cells use the same package, receipt, checkpoint, replay, duplicate, and source-position assertion surface as C1.

## Limits

This closes C2 only. Cross-destination chaos is now closed at `.10x/tickets/done/2026-07-08-p0-c3-cross-destination-chaos.md`; per-destination live-run goldens later closed at `.10x/tickets/done/2026-07-08-p0-c4-live-run-goldens-per-destination.md`; property/fuzz targets later closed at `.10x/tickets/done/2026-07-08-p0-c5-property-fuzz-targets.md`; Workstream C closure remains owned by `.10x/tickets/2026-07-08-p0-c6-workstream-c-closure.md`.

CodeQL was not run for this slice. C2 touched conformance harness/test code and internal dev-dependency references, not production runtime or a new security boundary; the reusable CodeQL database was not recreated, and the local security/supply-chain checks above were run instead.
