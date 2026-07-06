Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-golden-package-conformance-foundation.md, .10x/tickets/2026-07-05-conformance-chaos-golden.md, .10x/specs/package-lifecycle-determinism.md, .10x/specs/conformance-governance-roadmap.md

# Golden package conformance foundation evidence

## What was observed

The first conformance-owned golden-package harness was implemented under `crates/firn-conformance/src/golden_package/` with a committed expected fixture at `crates/firn-conformance/golden/prepared-orders-v1/expected.json`.

The committed `prepared-orders-v1` expectation contains:

- package hash `sha256:6ce1b03f1138777b3a521362d56533d8f509ed6e84973738bd372f37e3bf36a8`
- lifecycle status `packaged`
- signature signing input equal to the package hash
- identity manifest version `1`
- 15 identity file entries with byte counts and SHA-256 values
- 1 segment entry, `seg-000001`, with 3 rows, 1082 bytes, and SHA-256 `3877d05802f03053efcdc1ba97244cdb449cc2fdcf9e91d051f85e52fef1cdc1`

The harness builds the fixture through public `firn-package` APIs, verifies the package with `PackageReader::verify`, derives evidence from the verified manifest, and compares manifest version, package hash, lifecycle status, signature signing input/value, identity manifest version, identity layout, identity file set/hash/byte count, and segment set/path/row count/byte count/hash.

Negative self-tests corrupt committed-evidence fields and prove the harness fails for package hash, missing and extra identity files, file hash, file byte count, segment hash, segment byte count, segment row count, lifecycle status, signing input, and identity layout. A tampered segment test proves the golden assertion path verifies package integrity before comparing evidence.

## Procedure

Focused ticket checks:

- `cargo fmt --all -- --check` passed.
- `git diff --check` passed.
- `cargo test -p firn-conformance --locked --no-fail-fast` passed: 27 tests.
- `cargo clippy -p firn-conformance --all-targets --locked -- -D warnings` passed.
- `cargo test -p firn-package --locked --no-fail-fast` passed: 13 tests.
- Bounded mutation check passed:
  `cargo mutants --package firn-conformance --file crates/firn-conformance/src/golden_package/mod.rs --test-package firn-conformance --output target/quality/mutants-golden-package --no-shuffle --jobs 4 --timeout 120 -- --locked golden_package`
  produced 18 mutants total: 11 caught, 7 unviable, 0 missed, 0 timeouts.

Workspace correctness and feature checks:

- `cargo check --workspace --all-targets --locked` passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast` passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` passed.
- `cargo nextest run --workspace --locked` passed: 230 tests, run id `f723e624-7f9d-48cc-9813-7e3bc8523334`.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast` passed: 0 doctests.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked` passed.
- `cargo check --workspace --all-targets --all-features --locked` passed.
- `cargo check --workspace --all-targets --no-default-features --locked` passed.
- `cargo hack check --workspace --all-targets --each-feature --locked` passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` passed.
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings` passed.
- `cargo semver-checks --workspace --baseline-rev HEAD` passed for all workspace crates with no semver update required.

Dependency, supply-chain, and scanner checks:

- `cargo audit` passed after scanning 402 dependencies.
- `cargo deny check` passed. It continued to report the known duplicate Arrow 58/59 dependency family as warnings, with advisories, bans, licenses, and sources accepted.
- `cargo vet` passed: `Vetting Succeeded (385 exempted)`.
- `cargo machete` passed with no unused dependencies.
- `CARGO_TARGET_DIR=target/quality/udeps-golden-package-target cargo +nightly udeps -p firn-conformance --all-targets --locked` passed: all deps used, including the new `serde` dependency.
- `cargo metadata --format-version=1 --locked > reports/ai-quality/cargo-metadata-golden-package.json` passed.
- `cargo tree --workspace --locked > reports/ai-quality/cargo-tree-golden-package.txt` passed.
- `cargo tree --workspace --locked -d > reports/ai-quality/cargo-tree-duplicates-golden-package.txt` passed and captured the existing duplicate-dependency report.
- `tools/codeql-rust-quality.sh` passed and reused the fresh database at `target/quality/codeql-db-rust`; `target/quality/reports/codeql-rust-current.sarif` had 0 SARIF findings. CodeQL reported the known local Rust extractor macro-diagnostic noise from `.10x/knowledge/quality-gate-execution.md`: extraction errors 0, extraction warnings 1926, 140 Rust files scanned.
- `osv-scanner scan source -r . --format json --output reports/ai-quality/osv-golden-package.json` passed with 0 results.
- `semgrep scan --config p/rust --error --json --output reports/ai-quality/semgrep-rust-golden-package.json --exclude target --exclude reports .` passed with 0 results.
- `gitleaks git --no-banner --redact --report-format json --report-path reports/ai-quality/gitleaks-git-golden-package.json .` passed with 0 findings.
- A clean-tree `gitleaks dir` pass over a temporary copy built from `git ls-files --cached --others --exclude-standard` passed with 0 findings.

Soundness, coverage, and maintainability checks:

- `cargo llvm-cov --workspace --all-features --locked --summary-only` passed. Workspace totals were 78.44% regions and 81.16% lines. The new `crates/firn-conformance/src/golden_package/mod.rs` measured 89.90% regions and 92.61% lines.
- `CARGO_TARGET_DIR=target/quality/careful-golden-package-target cargo +nightly careful test -p firn-conformance --all-features --locked` passed: 27 tests and 0 doctests.
- Direct unsafe/FFI/raw-pointer scan over `crates tools python` found only the existing string literal `crates/firn-http/src/retry.rs:115: "not retrying unsafe unit..."`; no owned `unsafe`, FFI, raw pointer, `MaybeUninit`, `NonNull`, `UnsafeCell`, `from_raw`, `into_raw`, or `transmute` use was introduced by this slice.
- `rust-code-analysis-cli -p crates/firn-conformance/src/golden_package -m -O json --pr -o reports/ai-quality/rust-code-analysis-golden-package` passed and wrote JSON metrics for `mod.rs` and `tests.rs`.
- `jscpd crates/firn-conformance/src/golden_package --format rust --reporters console,json --output reports/ai-quality/jscpd-golden-package --min-lines 12 --min-tokens 80 --threshold 0 --exit-code 0 --no-colors` found 0 clones across 2 Rust files, 582 lines, and 3504 tokens.

## Limits

`cargo geiger` was not run because `.10x/knowledge/quality-gate-execution.md` records that it can clean normal Cargo build output in this repository and may fail on dependency scan warnings even when firn-owned code has no `unsafe`. The direct source scan plus `cargo careful` covered the relevant owned-code soundness risk for this no-unsafe conformance harness.

`cargo miri`, `cargo fuzz`, and `cargo kani` were not run for this slice. The change introduces no unsafe code, no parser or arithmetic core, and no fuzz/Kani harness target; the executable oracle is the deterministic golden-package test suite plus negative self-tests and bounded mutation testing.

Performance and binary-size tools such as `cargo bench`, `criterion`, `cargo bloat`, flamegraphs, heap profilers, and sanitizers were not run because this slice adds a deterministic conformance harness and committed fixture, not a production hot path, allocator-sensitive runtime, or binary-size-sensitive executable.

Generated scanner and metrics artifacts remain under ignored `reports/ai-quality/` or `target/quality/` paths and are not project authority; this evidence record is the durable summary.

## What this supports

The ticket acceptance criteria are satisfied for the first golden-package conformance foundation: package evidence is committed, verified before comparison, compared hash-by-hash, proven deterministic across 100 local rebuilds, and hardened with negative self-tests plus mutation testing. Full live-run golden fixtures, cross-OS golden stability, CLI golden update behavior, archive persistence, and MVP killer-demo evidence remain owned by `.10x/tickets/2026-07-05-conformance-chaos-golden.md`.
