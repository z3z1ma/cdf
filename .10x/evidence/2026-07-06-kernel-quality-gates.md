Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/2026-07-06-ratify-supply-chain-policy.md

# Kernel quality gate evidence

## What was observed

The `firn-kernel` child implementation and workspace scaffold were verified with the `QUALITY.md` standard/deep loop after tool installation. The changed kernel package exposes only Arrow, `futures-core`, `serde`, and standard/runtime-neutral Rust types at its direct dependency boundary.

Parent-observed mutation testing after the final repair reported `38 mutants tested in 47s: 21 caught, 17 unviable`, with zero missed mutants.

## Procedure

All commands below were run from `/Users/alexanderbut/code_projects/personal/firn`.

Tool installation and inventory:

```text
cargo install --locked cargo-nextest cargo-llvm-cov cargo-hack cargo-deny cargo-audit cargo-vet cargo-machete cargo-semver-checks cargo-geiger cargo-bloat cargo-mutants cargo-careful cargo-fuzz rust-code-analysis-cli
cargo install --locked cargo-udeps
brew install semgrep
cargo --list | rg 'nextest|llvm-cov|hack|deny|audit|vet|machete|udeps|semver|geiger|bloat|mutants|careful|fuzz'
command -v rust-code-analysis-cli semgrep osv-scanner gitleaks codeql jscpd
```

The listed cargo tools were installed and available. External tools available were `rust-code-analysis-cli`, `semgrep`, `osv-scanner`, `gitleaks`, `codeql`, and `jscpd`. `cargo-llvm-cov` installed `llvm-tools-preview` for `stable-aarch64-apple-darwin` when coverage first ran. Tool install commands emitted warnings about yanked crates in some tools' own published lockfiles; these were not findings against this repository.

Compile, lint, test, and docs:

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo check --workspace --all-targets --all-features --locked
cargo check --workspace --all-targets --no-default-features --locked
cargo hack check --workspace --all-targets --each-feature --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings
cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings
cargo test -p firn-kernel --locked --no-fail-fast
cargo test --workspace --all-targets --locked --no-fail-fast
cargo test --workspace --all-targets --all-features --locked --no-fail-fast
cargo nextest run --workspace --locked
cargo test --workspace --doc --all-features --locked --no-fail-fast
cargo doc --workspace --all-features --no-deps --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
```

All commands passed. After the mutation repair, workspace tests and Nextest ran 7 kernel tests with no failures.

Architecture, coverage, dependency hygiene, and metrics:

```text
cargo metadata --format-version=1 --locked > reports/ai-quality/cargo-metadata.json
cargo tree --workspace --locked > reports/ai-quality/cargo-tree.txt
cargo tree --workspace --locked -d > reports/ai-quality/cargo-tree-duplicates.txt
cargo tree -p firn-kernel --depth 1 --locked
rg -n "DataFusion|datafusion|DuckDB|duckdb|PyO3|pyo3|Python|python|Tokio|tokio|reqwest|rusqlite|clap|object_store|firn_engine|firn_cli|firn_project" crates/firn-kernel/src crates/firn-kernel/Cargo.toml
cargo llvm-cov --workspace --all-features --locked --summary-only
cargo llvm-cov --workspace --all-features --locked --json --output-path reports/ai-quality/llvm-cov.json
cargo machete
rust-code-analysis-cli -m -p crates -O json -o reports/ai-quality/rust-code-analysis
jscpd . --reporters json,console --output reports/ai-quality/jscpd --ignore "**/target/**,**/.git/**,**/reports/**"
cargo bloat --release -p firn-cli --bin firn-cli -n 20
```

All hard gates in this group passed. Direct `firn-kernel` dependencies were `arrow-array`, `arrow-schema`, `futures-core`, `serde`, and dev-only `serde_json`. The forbidden upper-layer scan returned no matches. Active-target duplicate dependency output was empty. Coverage after the first repair was 69.82% line, 72.68% region, and 51.35% function coverage for currently implemented code. `cargo machete` found no unused dependency candidates. `jscpd` reported zero Rust duplication; clone findings were in `QUALITY.md` examples and review prose. `cargo bloat` showed the scaffolded CLI at 450.9 KiB file size and 207.1 KiB `.text`.

Security, supply chain, public API, and unsafe:

```text
cargo audit
cargo deny check advisories
cargo deny check
osv-scanner scan source -r . --format json --output reports/ai-quality/osv.json
gitleaks git --no-banner --redact --report-format json --report-path reports/ai-quality/gitleaks-git.json .
gitleaks dir --no-banner --redact --report-format json --report-path reports/ai-quality/gitleaks-dir.json .
semgrep scan --config p/rust --error --json --output reports/ai-quality/semgrep-rust.json --exclude target --exclude reports .
codeql database create reports/ai-quality/codeql-db --language=rust --source-root . --overwrite --command 'cargo check --workspace --all-targets --locked'
codeql database analyze reports/ai-quality/codeql-db codeql/rust-queries --format=sarif-latest --output=reports/ai-quality/codeql-rust.sarif
cargo geiger --all-features
cargo geiger --all-features --manifest-path /Users/alexanderbut/code_projects/personal/firn/crates/firn-kernel/Cargo.toml
rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|Unpin|Send|Sync" crates --glob '!target/**'
cargo semver-checks -p firn-kernel
cargo vet
```

`cargo audit`, `cargo deny check advisories`, OSV, Gitleaks, Semgrep, and CodeQL reported zero vulnerability/secret/security findings. CodeQL successfully created and analyzed a Rust database; SARIF had zero results, with extractor diagnostic warnings recorded as a tool limitation. `cargo geiger` could not run against the virtual workspace manifest; the kernel-manifest run reported `firn-kernel` itself as `0/0` unsafe, while transitive Arrow/dependency crates contain unsafe. The direct source search found no unsafe, FFI, raw-pointer, transmute, or manual Send/Sync impls; only `Send` bounds in `BatchStream`/`BoxFuture` matched.

Full `cargo deny check` failed only because no repository `deny.toml` exists, so the default config has no ratified allowed-license list and rejects even Apache-2.0/MIT licenses. `cargo vet` failed because `supply-chain/` is not initialized. These are policy-adoption gaps tracked by `.10x/tickets/2026-07-06-ratify-supply-chain-policy.md`, not kernel implementation failures. `cargo semver-checks -p firn-kernel` could not run because `firn-kernel` is not published and no semver baseline revision exists.

Mutation:

```text
cargo mutants -p firn-kernel --test-tool nextest --timeout 60 --minimum-test-timeout 5 -j 4 -o reports/ai-quality/mutants-kernel --cargo-arg=--locked
cargo mutants -p firn-kernel --test-tool nextest --timeout 60 --minimum-test-timeout 5 -j 4 -o reports/ai-quality/mutants-kernel-parent --cargo-arg=--locked
```

The first run found five missed mutants in `FirnError` display, `SourcePosition::version`, and negative `Receipt::covers_state_delta` behavior. The worker added focused tests. The parent-observed rerun passed with zero missed mutants: `38 mutants tested in 47s: 21 caught, 17 unviable`.

## What this supports or challenges

This supports closing `.10x/tickets/done/2026-07-05-kernel-core-types.md`: acceptance criteria are covered by source inspection, unit tests, dependency-boundary checks, workspace compile/lint/test gates, security scans, and mutation testing.

This challenges the broader repository's supply-chain policy completeness: `cargo deny` license policy and cargo-vet adoption need ratification before those full policy gates can pass.

## Limits

Miri, cargo-careful, sanitizers, fuzzing, Kani, `cargo bench`, and `cargo udeps` were not run. The implemented firn source contains no unsafe/FFI/concurrency primitive code, no fuzz targets, no Kani harnesses, no benchmark suites, and no nightly toolchain requirement for this ticket. `cargo udeps` requires nightly; `cargo machete` was used for stable unused-dependency detection.

Temporary `reports/ai-quality/` artifacts were generated for tool execution and summarized here. The durable evidence is this record, not the generated report directory.
