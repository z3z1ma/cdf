Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-live-local-file-run-golden-conformance.md, .10x/tickets/2026-07-05-conformance-chaos-golden.md, .10x/specs/conformance-governance-roadmap.md, .10x/specs/package-lifecycle-determinism.md, .10x/specs/checkpoint-state-cdf-line.md, .10x/specs/destination-receipts-guarantees.md

# Live local-file run golden conformance evidence

## What was observed

`cdf-conformance` now exports a focused `live_run` module from a thin crate root. The module builds a deterministic declarative local-file resource, runs it through the public `cdf_project::run_local_file_to_duckdb_checkpoint` primitive, records committed golden evidence in `crates/cdf-conformance/golden/live-local-file-v1/expected.json`, and reuses the existing package replay assertions for DuckDB receipt durability, SQLite checkpoint heads, destination mirror rows, recovery, and duplicate replay identity.

The implemented live fixture uses explicit package, checkpoint, pipeline, resource, target, file-content, source hash, and row-count constants. Tests prove 100 local rebuilds produce the same package evidence; post-receipt failure leaves a durable verified receipt with no committed checkpoint head; recovery can commit from the durable receipt after the source file is removed; duplicate package replay is a no-op for destination rows and mirror state; and negative self-tests fail on corrupted package hash, wrong source metadata, missing checkpoint commit, missing receipt durability, and wrong destination row counts.

The slice did not edit production runtime behavior, did not change `Cargo.lock`, did not add native arrow-rs `parquet` or `paste`, and did not change supply-chain policy.

## Procedure and results

Focused ticket gates:

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo test -p cdf-conformance --locked --no-fail-fast`: passed, 36 tests.
- `cargo test -p cdf-project --locked --no-fail-fast`: passed, 29 tests.
- `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`: passed.

Workspace and `QUALITY.md` gates:

- `cargo check --workspace --all-targets --locked`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo test --workspace --locked --no-fail-fast`: passed.
- `cargo nextest run --workspace --locked`: passed, 293 tests.
- `cargo doc --workspace --no-deps --locked`: passed.
- `cargo hack check --workspace --feature-powerset --locked`: passed.
- `cargo deny check && cargo audit && cargo vet --locked`: passed. `cargo deny` still reports duplicate-version warnings, but advisories, bans, licenses, and sources passed; `cargo audit` scanned 429 dependencies; `cargo vet` succeeded with the current exemption backlog.
- `osv-scanner scan source -r . --format json --output target/quality/reports/osv-live-run-conformance.json`: passed.
- `semgrep scan --config auto --error --json --no-git-ignore --output target/quality/reports/semgrep-live-run-conformance-allfiles.json crates/cdf-conformance .10x/tickets/done/2026-07-06-live-local-file-run-golden-conformance.md .10x/knowledge/cdf-product-objective.md`: passed with 0 findings over 17 files.
- Source-only `gitleaks dir` over a temporary mirror of tracked and new source files passed with no leaks, with report `target/quality/reports/gitleaks-live-run-conformance-source.json`. A full workspace `gitleaks dir .` run found generated/build-output noise and is not used as source evidence.
- `tools/codeql-rust-quality.sh`: passed using the existing reusable database at `target/quality/codeql-db-rust`. `target/quality/reports/codeql-rust-current.sarif` had 0 non-note findings. Extractor metrics reported 0 extraction errors, 2332 warnings, 149 files extracted, 113 files with extractor warnings, 36 files without warnings, and 2646 unresolved macro calls, matching the known CodeQL Rust extractor limitation in `.10x/knowledge/quality-gate-execution.md`.
- `cargo machete`: passed with no unused dependencies.
- `cargo semver-checks check-release --workspace --all-features --baseline-rev HEAD`: passed. The initial crates.io-baseline form is structurally inapplicable because workspace crates are unpublished.
- `cargo +nightly udeps --workspace --all-targets --locked`: passed with all dependencies used.
- Direct first-party unsafe scans over `crates/**/*.rs` found no `unsafe {` blocks. The only `unsafe` word match was a string in `crates/cdf-http/src/retry.rs`.
- `jscpd crates --reporters json --output target/quality/reports/jscpd-live-run-conformance`: passed as a duplication metrics gate; duplicated lines were 3.8584378375839554% across `crates`.
- `cargo llvm-cov nextest --workspace --locked --lcov --output-path target/quality/reports/llvm-cov-live-run-conformance.lcov`: passed, 293 tests.
- `cargo mutants --package cdf-conformance --file crates/cdf-conformance/src/live_run/mod.rs --jobs 4 --timeout 120 --output target/quality/mutants-live-run-conformance -- --locked`: passed with 14 mutants tested, 7 caught, 7 unviable, 0 missed.
- `cargo +nightly careful test -p cdf-conformance --locked --no-fail-fast`: passed, 36 tests. The stable `cargo careful` invocation is structurally inapplicable because it requires nightly `-Z` support.
- `rust-code-analysis-cli -p crates/cdf-conformance/src/live_run/mod.rs -p crates/cdf-conformance/src/live_run/tests.rs -m -O json -o target/quality/reports/rust-code-analysis-live-run-conformance`: passed.
- `cargo geiger --manifest-path /Users/alexanderbut/code_projects/personal/cdf/crates/cdf-conformance/Cargo.toml --all-features --locked`: rebuilt and scanned but exited 1 with dependency parser/unscanned-file warnings and `error: Found 409 warnings`. This matches the known Geiger limitation in `.10x/knowledge/quality-gate-execution.md`; the direct owned-source unsafe scan and `cargo careful` are the accepted first-party unsafe signals for this slice.

## What this supports

This supports closing `.10x/tickets/done/2026-07-06-live-local-file-run-golden-conformance.md`: the conformance suite now covers the first live local-file execution path end to end, records deterministic golden package evidence, verifies receipts and checkpoint state, proves the CDF-line committed-before-checkpoint recovery window without rereading the source file, proves duplicate/no-op replay behavior, and has negative and mutation evidence that the harness catches material gaps.

The evidence also supports the user's CodeQL reuse requirement for this slice: the wrapper reused `target/quality/codeql-db-rust`, and a post-Geiger directory check confirmed the reusable database remained present.

## Limits

- `cargo geiger` remains a noisy local dependency-warning gate for this repository state. It is recorded as a tool limit, not as evidence of first-party unsafe code.
- CodeQL Rust extraction retains known macro diagnostics while SARIF findings and extraction errors are 0.
- This slice covers a deterministic local-file-to-DuckDB/SQLite live run. It does not prove HTTP/API source execution, SQL source execution, full lifecycle process-kill chaos, run-ledger defaults, CLI `resume`, or native Arrow/DataFusion Parquet policy.
