Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p0-c4-live-run-goldens-per-destination.md, .10x/tickets/2026-07-07-p0-workstream-c-spine-conformance-harness.md

# P0 C4 live-run goldens per destination evidence

## What was observed

`cdf-conformance::live_run` now commits and verifies live `run_project` golden fixtures for each MVP destination:

- DuckDB: `crates/cdf-conformance/golden/live-local-file-v1/expected.json`
- filesystem Parquet: `crates/cdf-conformance/golden/live-local-file-parquet-v1/expected.json`
- Postgres: `crates/cdf-conformance/golden/live-local-file-postgres-v1/expected.json`

The committed package hashes are:

- DuckDB `live-local-file-v1`: `sha256:fe59f30020872aa7d918de63408b070ffe0e61983f2431c4978e61fc05314e67`
- filesystem Parquet `live-local-file-parquet-v1`: `sha256:6bf121eb1151a7cad1468402671446f769300b73ddaa992dbce694dbdcb37cbe`
- Postgres `live-local-file-postgres-v1`: `sha256:2c39d0a79a2bd936ebff4058c4b659cb71edf6f000a94d51d356c53907c30cbb`

Each fixture records package id, top-level package hash, checkpoint id, pipeline id, resource id, destination, destination target, source position path/hash/size, destination row counts, segment count, and nested package evidence.

The live-run golden comparison verifies the package before comparing the expected evidence by calling `assert_verified_package_matches_golden`.

Receipt verification is trait-level for all three destinations through `DestinationProtocol::verify`.

Determinism coverage:

- DuckDB live local file golden: 100 reruns.
- filesystem Parquet live local file golden: 100 reruns.
- Postgres live local file golden: 10 bounded reruns against a real reset schema.

Postgres repeats are intentionally bounded because each iteration resets and exercises a real database schema. DuckDB and Parquet keep the 100-run deterministic golden proof.

## Procedure

Implementation review inspected these files:

- `crates/cdf-conformance/src/live_run/mod.rs`
- `crates/cdf-conformance/src/live_run/tests.rs`
- `crates/cdf-conformance/src/live_run/destinations.rs`
- `crates/cdf-conformance/src/live_run/evidence.rs`
- `crates/cdf-conformance/golden/live-local-file-v1/expected.json`
- `crates/cdf-conformance/golden/live-local-file-parquet-v1/expected.json`
- `crates/cdf-conformance/golden/live-local-file-postgres-v1/expected.json`

Commands run:

```text
cargo fmt --all --check
cargo test --locked -p cdf-conformance live_run -- --nocapture
cargo test --locked --no-fail-fast -p cdf-conformance golden
cargo nextest run --locked -p cdf-conformance live_run
cargo check -p cdf-conformance --all-targets --locked
cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings
git diff --check
jscpd crates/cdf-conformance/src/live_run --reporters json,console --output target/quality/reports/jscpd-p0-c4-live-run-src --ignore "**/target/**,**/.git/**,**/reports/**"
jscpd crates/cdf-conformance/golden/live-local-file-v1 crates/cdf-conformance/golden/live-local-file-parquet-v1 crates/cdf-conformance/golden/live-local-file-postgres-v1 --reporters json,console --output target/quality/reports/jscpd-p0-c4-live-run-goldens --ignore "**/target/**,**/.git/**,**/reports/**"
rust-code-analysis-cli -m -O json -p crates/cdf-conformance/src/live_run > target/quality/reports/rust-code-analysis-p0-c4-live-run.json
semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-p0-c4-live-run.json crates/cdf-conformance/src/live_run crates/cdf-conformance/golden/live-local-file-v1 crates/cdf-conformance/golden/live-local-file-parquet-v1 crates/cdf-conformance/golden/live-local-file-postgres-v1
gitleaks dir --redact --no-banner --report-format json --report-path target/quality/reports/gitleaks-p0-c4-live-run-src.json crates/cdf-conformance/src/live_run
gitleaks dir --redact --no-banner --report-format json --report-path target/quality/reports/gitleaks-p0-c4-live-run-goldens.json crates/cdf-conformance/golden
cargo deny check > target/quality/reports/cargo-deny-p0-c4-live-run.txt
cargo vet --locked > target/quality/reports/cargo-vet-p0-c4-live-run.txt
cargo audit --deny warnings --ignore RUSTSEC-2024-0436 > target/quality/reports/cargo-audit-p0-c4-live-run.txt
rg -n "/Users|postgres://|postgresql://|127\\.0\\.0\\.1|localhost|TEST_DATABASE_URL|password|secret|/var/folders|/tmp/" crates/cdf-conformance/golden/live-local-file-v1 crates/cdf-conformance/golden/live-local-file-parquet-v1 crates/cdf-conformance/golden/live-local-file-postgres-v1
```

The first attempted multi-source `gitleaks dir` invocation did not complete promptly and was interrupted. The split source/golden scans above completed and passed.

## Results

- `cargo fmt --all --check`: pass.
- `cargo test --locked -p cdf-conformance live_run -- --nocapture`: pass; 7 tests passed, 0 failed, 39 filtered out.
- `cargo test --locked --no-fail-fast -p cdf-conformance golden`: pass; 7 tests passed, 0 failed, 39 filtered out.
- `cargo nextest run --locked -p cdf-conformance live_run`: pass; 7 tests passed, 39 skipped.
- `cargo check -p cdf-conformance --all-targets --locked`: pass.
- `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`: pass.
- `git diff --check`: pass.
- `jscpd` over `src/live_run`: pass; 4 Rust files, 1263 lines, 7448 tokens, 0 clones, 0.00% duplicated lines/tokens.
- `jscpd` over the three committed golden JSON fixture directories: pass command with 6 clones, 144 duplicated lines, 384 duplicated tokens, 45.00% duplicated lines, 41.33% duplicated tokens. This duplication is accepted for C4 because these are intentionally repeated committed golden evidence schemas, not source code clones.
- `rust-code-analysis-cli`: report written to `target/quality/reports/rust-code-analysis-p0-c4-live-run.json`; 4 files, 69 functions. Max function cyclomatic complexity was 12 in `run_live_local_file_fixture_with_destination`; max function cognitive complexity was 4.
- `semgrep scan --config p/rust`: pass; 7 targets scanned, 11 Rust rules, 0 findings.
- `gitleaks dir` over `crates/cdf-conformance/src/live_run`: pass; no leaks found.
- `gitleaks dir` over `crates/cdf-conformance/golden`: pass; no leaks found.
- `cargo deny check`: pass; stdout ended with `advisories ok, bans ok, licenses ok, sources ok`. It emitted already-known duplicate Arrow 58/59 warnings covered by Workstream D records.
- `cargo vet --locked`: pass; `Vetting Succeeded (393 exempted)`.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: pass.
- Fixture secret/path scan: no matches for local absolute paths, Postgres URLs, localhost URLs, password/secret markers, or temp paths.

CodeQL was not rerun for C4 because this slice changed conformance harness and committed golden evidence only, did not change production runtime or dependency posture, and `.10x/knowledge/quality-gate-execution.md` says to reuse the expensive database only when CodeQL is needed for the current source/risk. The existing reusable database was not recreated.

## What this supports

This supports closing `.10x/tickets/done/2026-07-08-p0-c4-live-run-goldens-per-destination.md`:

- DuckDB, filesystem Parquet, and Postgres each have a committed live-run golden generated from a live `run_project` cell.
- Each golden carries package identity, checkpoint identity, destination target, source position evidence, segment count, destination row counts, and nested package evidence.
- Golden comparison verifies the actual package before comparing expected evidence.
- Determinism is proven by repeated runs with stable evidence for each fixture.
- Trait-level receipt verification is exercised for all three destinations.

## Limits

The top-level `mirror_load_rows` and `mirror_state_rows` fields remain in `LiveRunGoldenEvidence` for compatibility with the existing DuckDB live-run golden shape. Portable cross-destination assertions use `destination_row_counts`; Parquet and Postgres do not rely on the legacy mirror field names.

Postgres uses 10 deterministic reruns rather than 100 because each run resets and exercises a real database schema. The focused test still verifies stable package evidence and destination row counts across the bounded repeat set.
