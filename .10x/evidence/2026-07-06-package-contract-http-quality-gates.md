Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-contract-compiler-normalization.md, .10x/tickets/done/2026-07-05-package-builder-reader.md, .10x/tickets/done/2026-07-05-http-toolkit.md, .10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md, .10x/knowledge/quality-gate-execution.md

# Package, contract, and HTTP QUALITY evidence

## What was observed

The integrated contract compiler, package builder/reader, and HTTP toolkit batch passed formatting, compile, feature, clippy, test, doctest, rustdoc, dependency, security, secret, semver, coverage, and CodeQL checks that are applicable without ratifying new repository policy.

Parent review fixed one contract normalizer edge before closure: exact duplicate source names now hard-error instead of allowing duplicate normalized output names.

Generated reports were written under `target/quality/reports/2026-07-06-package-contract-http/`. The reusable CodeQL database is `target/quality/codeql-db-rust`.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

- `cargo fmt --all -- --check`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo check --workspace --all-targets --all-features --locked`: passed.
- `cargo check --workspace --all-targets --no-default-features --locked`: passed.
- `cargo hack check --workspace --all-targets --each-feature --locked`: passed across 17 package feature checks.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings`: passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast`: passed, 45 tests.
- `cargo test --workspace --all-targets --all-features --locked --no-fail-fast`: passed, 45 tests.
- `cargo nextest run --workspace --locked`: passed, 45 tests.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed, 0 doctests.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo metadata --format-version=1 --locked`, `cargo tree --workspace --locked`, and `cargo tree --workspace --locked -d`: passed; duplicate tree printed nothing.
- `cargo machete`: passed, no unused dependency candidates.
- `rust-code-analysis-cli -m -p crates -O json`: passed.
- `jscpd ...`: completed; Rust duplicate metric was 163 duplicated lines / 2.38% duplicated tokens. Findings were limited to existing helper/test shapes and documentation/procedure repetition, so no implementation ticket was opened.
- `cargo llvm-cov --workspace --all-features --locked --summary-only`: passed; total line coverage 88.55%, region coverage 86.40%.
- `cargo audit --json`: passed; vulnerabilities count 0.
- `cargo deny check advisories`: passed.
- `osv-scanner scan source -r .`: passed; parsed report showed 0 vulnerability objects.
- `semgrep scan --config p/rust --error`: passed; 0 findings.
- `gitleaks dir --redact` and `gitleaks git --redact`: passed; both reports were empty arrays.
- `cargo semver-checks --baseline-rev HEAD~1`: passed; no semver update required.
- `rg` unsafe/source-surface search over `crates/`: no firn-owned `unsafe`, FFI, raw-pointer, transmute, or `MaybeUninit`; matches were `Send`/`Sync` bounds and a plain-text "unsafe unit" message.
- `cargo geiger` package-local attempt: produced a useful firn-owned `firn-kernel` 0/0 unsafe signal but exited nonzero due dependency scan warnings. Source search is the closure authority for firn-owned unsafe in this batch.
- Existing CodeQL DB analysis: passed with 0 SARIF results.
- Fresh CodeQL DB creation at `target/quality/codeql-db-rust`: passed.
- Fresh CodeQL analysis: passed with 0 SARIF results.
- `git diff --check`: passed.

## What this supports or challenges

Supports closure of the three child tickets: their code compiles across current feature shapes, tests pass through Cargo and nextest, public API semver did not regress against the previous commit, and security scanners found no actionable findings.

Supports the CodeQL reuse policy: the DB path is persistent under ignored target output, and the DB was recreated exactly once for this source/dependency-changing batch.

Challenges the current local quality workflow: CodeQL extraction metrics reported generated-target noise after semver/coverage runs, and `cargo geiger` cleaned normal Cargo output before failing on dependency scan warnings. `.10x/knowledge/quality-gate-execution.md` now records those workflow constraints.

## Limits

Full `cargo deny check` still fails because no ratified `deny.toml` license policy exists; `cargo vet` still fails because `supply-chain/` is absent. These are owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.

CodeQL reported extraction-warning metrics while still producing 0 findings. The warnings were dominated by generated target artifacts and macro-expansion limits, so this evidence supports security finding absence, not perfect CodeQL database quality.

No Miri, Kani, fuzzing, or benchmarks were run: this batch added no firn-owned unsafe, no configured fuzz/proof harness exists, and the work did not target performance-sensitive behavior.
