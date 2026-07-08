Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p0-c1-run-spine-matrix-foundation.md, .10x/tickets/2026-07-07-p0-workstream-c-spine-conformance-harness.md

# P0 C1 run-spine matrix foundation evidence

## What was observed

`cdf-conformance::run_matrix` now owns a FILE-source run-spine matrix model and a focused test that runs local file resources through `cdf_project::run_project` into DuckDB, filesystem Parquet, and Postgres destinations.

The final focused test executed 8 cells and recorded 1 sheet-backed exclusion:

| Source | Destination | Disposition | Result | Package | Checkpoint | Receipt |
| --- | --- | --- | --- | --- | --- | --- |
| file | duckdb | append | executed | `run-matrix-file-duckdb-append` | `checkpoint-run-matrix-file-duckdb-append` | `duckdb:events_append:sha256:3362dc549a9806c91f1260f214c6a6321a51c3a66e1c75310721ce1be43a950b` |
| file | duckdb | replace | executed | `run-matrix-file-duckdb-replace` | `checkpoint-run-matrix-file-duckdb-replace` | `duckdb:events_replace:sha256:270ce2b83a928546db5786bb7cd3fc3dddde6c969e9f43f3af6462d1bd627dcd` |
| file | duckdb | merge | executed | `run-matrix-file-duckdb-merge` | `checkpoint-run-matrix-file-duckdb-merge` | `duckdb:events_merge:sha256:b94055c0f3802e1e6ea890a15870a9de0a139f4651e8332264bbed64b387779c` |
| file | parquet_filesystem | append | executed | `run-matrix-file-parquet_filesystem-append` | `checkpoint-run-matrix-file-parquet_filesystem-append` | `parquet:events_append:sha256:988521cf127da02c0dc50b06c5fefcbc184033d453054f99985b3689adb7547b` |
| file | parquet_filesystem | replace | executed | `run-matrix-file-parquet_filesystem-replace` | `checkpoint-run-matrix-file-parquet_filesystem-replace` | `parquet:events_replace:sha256:9e5e27a14dc7ebc682a8e028d6f2ea3535e2dd66f471f4560973fb5772f6b376` |
| file | parquet_filesystem | merge | excluded | n/a | n/a | Parquet destination sheet supported_dispositions=[append, replace]; merge is not listed |
| file | postgres | append | executed | `run-matrix-file-postgres-append` | `checkpoint-run-matrix-file-postgres-append` | `postgres:cdf_conformance_run_matrix_2225_0.events_append:sha256e45efcab36ee91995e` |
| file | postgres | replace | executed | `run-matrix-file-postgres-replace` | `checkpoint-run-matrix-file-postgres-replace` | `postgres:cdf_conformance_run_matrix_2225_0.events_replace:sha256b890801ec1c7686510` |
| file | postgres | merge | executed | `run-matrix-file-postgres-merge` | `checkpoint-run-matrix-file-postgres-merge` | `postgres:cdf_conformance_run_matrix_2225_0.events_merge:sha256716b195d0a6eb0e6aa` |

Each executed cell asserts plan resource/package/scope honesty, `PackageReader::verify`, destination trait-level `DestinationProtocol::verify`, receipt-before-checkpoint gating through `after_receipt_verified`, committed checkpoint head, source position evidence, artifact replay identity, and duplicate replay no-op behavior.

Postgres coverage is mandatory in this harness. The first review found that the initial implementation could pass while excluding Postgres cells when local Postgres startup failed. That was repaired: `LivePostgres::start` now returns `Result<Self>`, setup/schema failures fail the test, and the test asserts the Postgres merge cell executes.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

```text
cargo fmt --all --check
cargo test -p cdf-conformance run_matrix -- --nocapture
cargo check -p cdf-conformance -p cdf-project --all-targets --locked
cargo clippy -p cdf-conformance -p cdf-project --all-targets --locked -- -D warnings
cargo nextest run -p cdf-conformance --locked
git diff --check
jscpd crates/cdf-conformance/src/run_matrix crates/cdf-conformance/src/lib.rs --reporters json,console --output target/quality/reports/jscpd-p0-c1-run-matrix --ignore "**/target/**,**/.git/**,**/reports/**"
rust-code-analysis-cli -m -O json -p crates/cdf-conformance/src/run_matrix > target/quality/reports/rust-code-analysis-p0-c1-run-matrix.json
scc --format json --output target/quality/reports/scc-p0-c1-run-matrix.json crates/cdf-conformance/src/run_matrix
gitleaks detect --no-git --source crates/cdf-conformance/src/run_matrix --report-format json --report-path target/quality/reports/gitleaks-p0-c1-run-matrix.json
semgrep scan --no-git-ignore --config p/rust --error --json --output target/quality/reports/semgrep-p0-c1-run-matrix.json crates/cdf-conformance/src/run_matrix
cargo deny check advisories
cargo audit
cargo vet --locked
cargo tree -p cdf-conformance --locked > target/quality/reports/cargo-tree-p0-c1-cdf-conformance.txt
```

## Results

- `cargo fmt --all --check`: passed.
- `cargo test -p cdf-conformance run_matrix -- --nocapture`: passed; final output recorded 8 executed cells and 1 excluded Parquet merge cell.
- `cargo check -p cdf-conformance -p cdf-project --all-targets --locked`: passed.
- `cargo clippy -p cdf-conformance -p cdf-project --all-targets --locked -- -D warnings`: passed.
- `cargo nextest run -p cdf-conformance --locked`: passed; 41 tests run, 41 passed, 0 skipped.
- `git diff --check`: passed.
- `jscpd`: passed; 2 Rust sources, 1,263 lines, 0 clones, 0 duplicated lines, 0 duplicated tokens.
- `rust-code-analysis-cli`: passed. Unit metrics: `run_matrix/mod.rs` 126 SLOC, cyclomatic 23, cognitive 7, max function cyclomatic 4; `run_matrix/tests.rs` 1,137 SLOC, cyclomatic 217, cognitive 36, max function cyclomatic 21.
- `scc`: passed; Rust total for `run_matrix`: 2 files, 1,263 lines, 1,240 code lines, aggregate complexity 10.
- `gitleaks`: passed; no leaks found.
- `semgrep --no-git-ignore`: passed; 2 files scanned, 11 Rust rules, 0 findings.
- `cargo deny check advisories`: passed.
- `cargo audit`: passed with one allowed warning, `RUSTSEC-2024-0436` for `paste 1.0.15`, matching the already-ratified advisory posture.
- `cargo vet --locked`: passed; 393 exempted.
- `cargo tree -p cdf-conformance --locked`: completed and wrote `target/quality/reports/cargo-tree-p0-c1-cdf-conformance.txt`.

## What this supports

This supports C1 acceptance for the FILE-source matrix foundation: conformance now owns the generic run-spine matrix shape; supported FILE-source cells execute through `run_project` across the current MVP destinations and dispositions; sheet exclusions are explicit; and executed cells assert the runtime/package/destination/checkpoint/replay/duplicate guarantees required by the ticket.

## Limits

This is C1 only. REST and SQL source archetypes remain owned by `.10x/tickets/2026-07-08-p0-c2-rest-sql-run-matrix.md`; cross-destination chaos remains owned by C3; per-destination live goldens remain owned by C4; property/fuzz targets remain owned by C5.

The C1 test harness is deliberately complete but large. The measured size/complexity and review concern are carried into C2, which now requires splitting the harness before adding REST/SQL cells.

CodeQL was not run for this slice. C1 touched conformance harness/test code and dev-dependency declarations, not production runtime or a new security boundary; the applicable local security and supply-chain checks above were run, and the reusable CodeQL database was not recreated unnecessarily.
