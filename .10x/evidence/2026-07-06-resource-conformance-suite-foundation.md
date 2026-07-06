Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-resource-conformance-suite-foundation.md, .10x/specs/resource-authoring-planning-batches.md, .10x/specs/conformance-governance-roadmap.md, .10x/tickets/2026-07-05-conformance-chaos-golden.md

# Resource conformance suite foundation evidence

## What was observed

`firn-conformance` now exposes `firn_conformance::resource`, a reusable planning-level harness for public `ResourceStream` and `QueryableResource` contracts. The harness verifies descriptor/schema coherence, partition-plan identity and scope honesty, declared capability preconditions, request identity preservation, mismatched-resource rejection, and pushed versus unsupported predicate classification.

`crates/firn-declarative` consumes the harness from REST, SQL, and file compiled-resource tests. The REST example expects cursor pushdown as inexact and a non-cursor predicate as unsupported; the SQL example expects exact predicate pushdown; the file example expects unsupported predicate classification.

The implementation remains planning-only. It does not call `CompiledResource::open`, read source data, prove partition-union completeness, validate replay suffix behavior, add chaos killpoints, add golden packages, or implement the MVP killer-demo harness.

## Procedure

Focused implementation checks:

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo test -p firn-conformance -p firn-declarative --locked --no-fail-fast`: passed.
- `cargo clippy -p firn-conformance -p firn-declarative --all-targets --locked -- -D warnings`: passed.
- `cargo nextest run -p firn-conformance -p firn-declarative --locked`: passed, 22 tests.
- `cargo mutants --package firn-conformance --test-package firn-conformance --test-package firn-declarative --file crates/firn-conformance/src/resource/mod.rs --no-shuffle --jobs 4 --timeout 120 --output target/quality/reports/mutants-resource-conformance -- --locked`: final run passed with 27 mutants tested, 22 caught, and 5 unviable. Earlier missed mutants led to additional negative self-tests before the final run.

Workspace quality checks from `QUALITY.md`:

- `cargo metadata --format-version=1 --locked`: passed and wrote `target/quality/reports/resource-conformance/cargo-metadata.json`.
- `cargo tree --workspace --locked`: passed and wrote `target/quality/reports/resource-conformance/cargo-tree.txt`.
- `cargo tree --workspace --locked -d`: passed and wrote `target/quality/reports/resource-conformance/cargo-tree-duplicates.txt`.
- `rustc --version --verbose`: rustc 1.96.1, host `aarch64-apple-darwin`, LLVM 22.1.2.
- `cargo --version --verbose`: cargo 1.96.1.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo check --workspace --all-targets --all-features --locked`: passed.
- `cargo check --workspace --all-targets --no-default-features --locked`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings`: passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast`: passed, 209 unit/integration tests.
- `cargo test --workspace --all-targets --all-features --locked --no-fail-fast`: passed, 209 unit/integration tests.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed.
- `cargo nextest run --workspace --locked`: passed, 209 tests.
- `cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo hack check --workspace --all-targets --each-feature --locked`: passed.
- `cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings`: passed.
- `cargo llvm-cov --workspace --all-features --locked --summary-only`: passed. Total line coverage was 80.47%; `crates/firn-conformance/src/resource/mod.rs` line coverage was 91.11%.
- `cargo semver-checks --workspace --baseline-rev HEAD`: passed for all workspace crates.
- `cargo audit`: passed with no vulnerabilities.
- `cargo deny check advisories`: passed.
- `cargo deny check`: passed with duplicate Arrow-family warnings and final advisories, bans, licenses, and sources all ok.
- `cargo vet`: passed with 385 exemptions.
- `osv-scanner scan source -r . --format json --output target/quality/reports/resource-conformance/osv.json`: passed.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/resource-conformance/semgrep-rust.json .`: passed with 0 findings.
- `semgrep scan --config p/security-audit --error --json --output target/quality/reports/resource-conformance/semgrep-security.json .`: passed with 0 findings.
- `gitleaks git --no-banner --redact --report-format json --report-path target/quality/reports/resource-conformance/gitleaks-git.json .`: passed with no leaks.
- `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/resource-conformance/gitleaks-dir.json /tmp/firn-gitleaks-resource-src`: passed with no leaks. The source snapshot was created from tracked and untracked non-ignored source files to avoid scanning generated `target/` artifacts.
- `cargo machete`: passed.
- `cargo +nightly udeps --workspace --all-targets`: passed.
- `rust-code-analysis-cli -m -p crates -O json -o target/quality/reports/resource-conformance/rust-code-analysis`: passed after using an output directory.
- `jscpd . --reporters json,console --output target/quality/reports/resource-conformance/jscpd --ignore "**/target/**,**/.git/**,**/reports/**"`: completed. Overall duplication was 4.08%; Rust duplication was 3.51%. Resource-harness clones were reviewed as fixture/test-helper repetition and not abstracted.
- Direct unsafe inventory with `rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|Unpin|Send|Sync" crates`: no Rust `unsafe`, FFI, raw-pointer, or transmute usage was found in the changed surface.
- `tools/codeql-rust-quality.sh`: passed. The reusable CodeQL database at `target/quality/codeql-db-rust` was refreshed because Rust source and lockfile inputs changed. SARIF results in `target/quality/reports/codeql-rust.sarif` contained 0 results.
- `cargo +nightly miri test -p firn-conformance --locked resource::`: passed.
- `cargo +nightly careful test -p firn-conformance -p firn-declarative --all-features --locked`: passed. The run warned that `libMainThreadChecker.dylib` was not found due local Xcode CLI tooling, but tests completed successfully.
- `cargo geiger` with absolute `--manifest-path` for `crates/firn-conformance` and `crates/firn-declarative`: passed. First-party package summaries reported 0 unsafe counts for functions, expressions, impls, and methods.

Tool availability notes:

- Installed quality tools observed: `cargo-nextest`, `cargo-hack`, `cargo-llvm-cov`, `cargo-mutants`, `cargo-deny`, `cargo-audit`, `cargo-vet`, `cargo-machete`, `cargo-semver-checks`, `osv-scanner`, `semgrep`, `gitleaks`, `codeql`, `rust-code-analysis-cli`, `jscpd`, `cargo-geiger`, `cargo-udeps`, `cargo-careful`, and `cargo-bloat`.
- `cargo fuzz list` failed because the workspace has no `fuzz/Cargo.toml`; no fuzz targets are configured for this slice.
- `cargo-kani`/`kani`, `cargo-flamegraph`, `tokei`, and `scc` were not installed and were not required to prove this planning-level library/test harness slice.

## What this supports or challenges

This evidence supports all acceptance criteria in `.10x/tickets/done/2026-07-06-resource-conformance-suite-foundation.md`:

- The public reusable resource harness exists and is consumed downstream by declarative REST, SQL, and file resource examples.
- Descriptor/schema coherence is checked for primary keys, merge keys, cursor fields, and schema-source evidence.
- Partition plans are checked for non-empty output, unique partition ids, compatible scopes, checkpoint scope shape, and request/resource identity.
- Negotiation is checked for mismatched-resource rejection, request preservation, partition preservation, pushed/unsupported classification, and expected exact/inexact pushdown fidelity.
- Incremental and replay capability claims are checked against the minimum public descriptor and state-shape preconditions currently expressible.
- Negative self-tests and mutation testing exercised false descriptor/schema claims, duplicate partition ids, unsupported-scope claims, mismatched requests, dishonest pushdown classification, mutated predicates, and invalid incremental/replay claims.

## Limits

This evidence does not prove the remaining parent-ticket behavior: source data execution, partition-union completeness, data boundedness, replay suffix correctness, chaos lifecycle killpoints, golden-package hash comparisons, MVP killer-demo behavior, or live Postgres destination conformance.

`tools/codeql-rust-quality.sh` reused the durable CodeQL database path but refreshed it because source and lockfile inputs changed. CodeQL extractor metrics still include known macro-expansion and `--lockfile-path` extraction warnings recorded in `.10x/knowledge/quality-gate-execution.md`; SARIF findings were 0.

The harness currently matches supported predicate operators by string containment because no predicate AST/parser contract exists in the public resource API. Negative tests and mutation testing cover the current behavior, but exact token semantics remain outside this planning-level foundation.
