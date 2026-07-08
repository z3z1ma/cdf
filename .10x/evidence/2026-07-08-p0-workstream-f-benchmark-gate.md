Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-07-p0-workstream-f-benchmark-gate.md

# P0 Workstream F benchmark gate evidence

## What was observed

P0 Workstream F added a private, non-published benchmark workspace member at `crates/cdf-benchmarks`. The harness owns deterministic fixture specs, Criterion smoke/full/postgres suites, fair-comparison labels, JSONL trend recording, explicit runtime budgets, and metric classes.

The benchmark crate is split into modules rather than a monolithic `lib.rs`: `catalog.rs`, `fixtures.rs`, `matrix.rs`, `resource.rs`, and `runners.rs`. `lib.rs` only declares modules, re-exports the public benchmark surface, and owns the shared error/result helper.

## Matrix coverage

Implemented local deterministic cells:

- direct Arrow filter/project;
- direct DataFusion filter/project;
- direct DuckDB-style local insert;
- CDF engine-to-package;
- file-to-package for CSV, JSON, NDJSON, and Parquet;
- Arrow IPC stream-to-package through the public stream reader;
- package replay to DuckDB;
- package replay to filesystem Parquet;
- REST decode from local fixture responses;
- package archive IPC-to-Parquet transcode;
- tiny startup file-to-DuckDB;
- medium and wide pipeline cases.

Implemented opt-in service-backed cell:

- package replay to Postgres through `ResolvedProjectDestination::postgres(...)`, gated by `CDF_BENCH_POSTGRES_URL` and excluded from normal smoke/full local runs.

Explicitly deferred cells:

- declarative `FileResource` Arrow IPC file input, because the current public file runtime rejects `arrow_ipc`; the harness covers the public Arrow IPC stream reader instead;
- native Polars comparison, because adding Polars would be a heavy non-MVP comparison dependency not required for the benchmark gate.

## Procedure

Focused build and test commands:

- `cargo fmt --all -- --check`
- `git diff --check`
- `cargo check -p cdf-benchmarks --all-targets --locked`
- `cargo clippy -p cdf-benchmarks --all-targets --locked -- -D warnings`
- `cargo test -p cdf-benchmarks --locked`

Benchmark and trend commands:

- `CDF_BENCH_SUITE=smoke cargo bench -p cdf-benchmarks --bench baseline --locked`
- `crates/cdf-benchmarks/scripts/record-trend.sh smoke target/cdf-benchmarks/trends/p0-f-smoke.jsonl`
- `CDF_BENCH_SUITE=full cargo bench -p cdf-benchmarks --bench baseline --locked`
- `crates/cdf-benchmarks/scripts/record-trend.sh full target/cdf-benchmarks/trends/p0-f-full.jsonl`
- disposable local Postgres via `initdb`/`pg_ctl`, then `CDF_BENCH_POSTGRES_URL=postgresql://cdf@127.0.0.1:<port>/postgres CDF_BENCH_SUITE=postgres cargo bench -p cdf-benchmarks --bench baseline --locked`
- same disposable Postgres URL with `crates/cdf-benchmarks/scripts/record-trend.sh postgres target/cdf-benchmarks/trends/p0-f-postgres.jsonl`

Quality/report commands:

- `jscpd crates/cdf-benchmarks --reporters json,console --output target/quality/reports/jscpd-p0-f-benchmarks --ignore "**/target/**,**/.git/**,**/reports/**"`
- `rust-code-analysis-cli -m -O json -p crates/cdf-benchmarks/src -p crates/cdf-benchmarks/benches -p crates/cdf-benchmarks/tests > target/quality/reports/rust-code-analysis-p0-f-benchmarks.json`
- `scc --format json crates/cdf-benchmarks > target/quality/reports/scc-p0-f-benchmarks.json`
- `rg -n "\bunsafe\b|extern \"|raw pointer|\*const|\*mut|unsafe impl|impl Send|impl Sync" crates/cdf-benchmarks > target/quality/reports/unsafe-rg-p0-f-benchmarks.txt || true`
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-p0-f-benchmarks.json crates/cdf-benchmarks`
- `semgrep scan --config p/security-audit --error --json --output target/quality/reports/semgrep-security-p0-f-benchmarks.json crates/cdf-benchmarks`
- `gitleaks detect --no-git --source crates/cdf-benchmarks --report-format json --report-path target/quality/reports/gitleaks-p0-f-benchmarks.json --no-banner --redact`
- `cargo audit --json > target/quality/reports/cargo-audit-p0-f-benchmarks.json`
- `cargo deny check > target/quality/reports/cargo-deny-p0-f-benchmarks.txt 2>&1`
- `cargo vet --locked > target/quality/reports/cargo-vet-p0-f-benchmarks.txt 2>&1`
- `osv-scanner scan source --lockfile Cargo.lock --format json > target/quality/reports/osv-p0-f-benchmarks.json`
- `tools/codeql-rust-quality.sh 2>&1 | tee target/quality/reports/codeql-p0-f-benchmarks.log`

## Results

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo check -p cdf-benchmarks --all-targets --locked`: passed.
- `cargo clippy -p cdf-benchmarks --all-targets --locked -- -D warnings`: passed.
- `cargo test -p cdf-benchmarks --locked`: passed; 3 focused fixture/matrix tests passed.
- Smoke Criterion suite: passed; log at `target/quality/reports/p0-f-benchmark-smoke.log`.
- Full Criterion suite: passed; log at `target/quality/reports/p0-f-benchmark-full.log`.
- Postgres Criterion suite: passed against disposable local Postgres; log at `target/quality/reports/p0-f-benchmark-postgres.log`.
- Smoke trend JSONL: 6 records at `target/cdf-benchmarks/trends/p0-f-smoke.jsonl`.
- Full trend JSONL: 16 records at `target/cdf-benchmarks/trends/p0-f-full.jsonl`.
- Postgres trend JSONL: 1 record at `target/cdf-benchmarks/trends/p0-f-postgres.jsonl`.
- `jscpd`: passed with 0 clones across 13 analyzed benchmark files.
- `rust-code-analysis-cli`: completed and wrote `target/quality/reports/rust-code-analysis-p0-f-benchmarks.json`.
- `scc`: completed and wrote `target/quality/reports/scc-p0-f-benchmarks.json`.
- Direct unsafe/FFI/raw-pointer scan: no matches.
- Semgrep Rust profile: passed with 0 findings after replacing direct `std::env::args*` parsing in the trend recorder with a feature-trimmed `clap` parser that validates raw option values.
- Semgrep security-audit profile: passed with 0 findings.
- Gitleaks source scan: passed with no leaks.
- `cargo audit`: passed with 0 vulnerabilities.
- `cargo deny check`: passed; report ends with `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo vet --locked`: passed after adding explicit exemptions, not audits, for the benchmark-only Criterion/Clap dependency subtree. Report contains `Vetting Succeeded (424 exempted)`.
- OSV: exited nonzero only for the already-ratified `paste` advisory `RUSTSEC-2024-0436`.
- CodeQL: reusable database at `target/quality/codeql-db-rust` was refreshed because Rust inputs/manifests/lockfile changed. Analysis exited 0; SARIF contained 0 results. Extractor metrics still show the known local Rust extractor macro warning pattern: 224 files extracted, 0 extraction errors, 3344 extraction warnings, and 4726 unresolved macro calls.

## What this supports

This supports closing P0 Workstream F. CDF now has an opt-in benchmark gate with first baseline numbers, trend recording, scoped dependency policy, and static quality evidence before follow-on performance tickets such as DuckDB bulk load, streaming commit optimization, or local partition parallelism claim deltas.

## Limits

Criterion timing output is local trend evidence only, not a public performance claim. The Postgres benchmark mutates a disposable database named by `CDF_BENCH_POSTGRES_URL`; normal smoke/full suites intentionally do not require a service. CodeQL's local Rust extractor retains the known macro-warning limitation recorded in `.10x/knowledge/quality-gate-execution.md`.
