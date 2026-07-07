Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-checkpoint-store-conformance-suite.md, .10x/specs/checkpoint-state-cdf-line.md, .10x/specs/conformance-governance-roadmap.md

# Checkpoint store conformance suite evidence

## What was observed

`cdf-conformance` now exposes a public checkpoint-store conformance harness over the public `CheckpointStore` trait. `cdf-state-sqlite` test integration runs both MVP stores, `InMemoryCheckpointStore` and `SqliteCheckpointStore`, through that reusable suite while preserving the existing SQLite-specific WAL, unique-head index, cross-connection uniqueness, row-corruption rejection, JSON round-trip, and unsupported-state-version tests.

The harness asserts receipt coverage for package hash, schema hash, every state segment, and segment row/byte counts; proposed and abandoned checkpoints not becoming heads; committed head lookup and history ordering; resource and scope isolation; rewind rejection for invalid targets; rewind marker append behavior without history deletion; head movement to the committed target; packages-ahead reporting from the current branch; and a public `Send + Sync` compile-time helper.

Parent review hardened the reusable harness with conformance self-tests built around intentionally faulty stores. These tests prove the harness fails when a candidate store is a no-op, accepts incorrect receipt row/byte counts in either direction, promotes proposed checkpoints, omits committed heads/history, accepts invalid rewinds, returns the wrong rewind report, or emits implausible timestamps.

## Procedure and results

- `cargo fmt --all -- --check` passed.
- `cargo test -p cdf-conformance --locked --no-fail-fast` passed: 10 unit tests, 0 doc-tests.
- `cargo test -p cdf-state-sqlite --locked --no-fail-fast` passed: 16 unit tests, 0 doc-tests.
- `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings` passed.
- `cargo clippy -p cdf-state-sqlite --all-targets --locked -- -D warnings` passed.
- Initial mutation checks exposed harness evidence gaps: a downstream-only `cargo mutants` run missed 9 mutants because the harness had no self-tests, and a combined harness/downstream run still missed 2 receipt count-direction mutants.
- After adding negative self-tests and both overreported and underreported row/byte assertions, `cargo mutants --package cdf-conformance --test-package cdf-conformance --test-package cdf-state-sqlite --file 'crates/cdf-conformance/src/checkpoint_store/*.rs' --no-shuffle --jobs 4 --timeout 120 --output target/quality/reports/mutants-checkpoint-conformance-final` reported 28 mutants tested: 18 caught, 10 unviable, 0 missed.
- `cargo check --workspace --all-targets --locked`, `cargo check --workspace --all-targets --all-features --locked`, and `cargo check --workspace --all-targets --no-default-features --locked` passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast` and `cargo test --workspace --all-targets --all-features --locked --no-fail-fast` passed, including 152 workspace tests.
- `cargo nextest run --workspace --locked` passed: 152 tests run, 152 passed.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast` passed.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked` passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`, and `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings` passed.
- `cargo hack check --workspace --all-targets --each-feature --locked` passed. Repository feature discovery found no package feature sections beyond the workspace member list.
- `cargo llvm-cov --workspace --all-features --locked --summary-only` passed: total region coverage 75.20%, total line coverage 78.29%; `cdf-conformance/src/checkpoint_store/fixtures.rs` reported 100.00% region and line coverage; `cdf-conformance/src/checkpoint_store/mod.rs` reported 91.73% region and 88.81% line coverage.
- `cargo +nightly careful test -p cdf-conformance --locked` and `cargo +nightly careful test -p cdf-state-sqlite --locked` passed; both emitted the local macOS `libMainThreadChecker.dylib` warning only.
- `cargo metadata --format-version=1 --locked`, `cargo tree --workspace --locked`, and `cargo tree --workspace --locked -d` passed with reports under ignored `target/quality/reports/`.
- `cargo machete` passed with no unused dependency candidates.
- `cargo +nightly udeps --workspace --all-targets --locked` passed: all deps seem to have been used.
- `rust-code-analysis-cli -m -p crates -O json -o target/quality/reports/rust-code-analysis-checkpoint-conformance` passed.
- `jscpd . --reporters json,console --output target/quality/reports/jscpd-checkpoint-conformance --ignore "**/target/**,**/.git/**,**/reports/**"` completed with 147 clones, 3.67% duplicated lines overall; Rust duplicated lines were 696, 2.45%.
- Direct owned-source unsafe search over `crates/` for unsafe blocks, unsafe impls, FFI, and raw pointers produced no matches. `cargo geiger` for the changed packages exited non-zero because of dependency parser warnings, but first-party rows for `cdf-conformance` and `cdf-state-sqlite` reported `0/0` unsafe.
- `cargo audit --json > target/quality/reports/cargo-audit-checkpoint-conformance.json` passed. `cargo deny check advisories` passed.
- `osv-scanner scan source -r . --format json --output target/quality/reports/osv-checkpoint-conformance.json` passed.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-checkpoint-conformance.json .` passed with 0 findings.
- `gitleaks git . --redact=100 --report-format json --report-path target/quality/reports/gitleaks-git-checkpoint-conformance.json` passed. A raw `gitleaks dir .` scan found generated `target/` and prior-report hits; rerunning against a temporary source-only copy excluding `.git`, `target`, and `reports` passed with 0 findings.
- `tools/codeql-rust-quality.sh` refreshed the reusable database at `target/quality/codeql-db-rust` because Rust sources/manifests changed, then analyzed it with `--rerun`. SARIF results length was 0. Extraction errors were 0; extraction warnings were 1350, matching the known Rust macro extractor limit recorded in `.10x/knowledge/quality-gate-execution.md`.
- `git diff --check -- . ':(exclude).gitignore'` passed.

## What this supports

The child ticket acceptance criteria are met for the reusable checkpoint-store conformance suite and MVP store integration. Required SQLite-only protections remain in the state-sqlite crate tests. The final mutation result supports that the reusable harness is not merely compiled, but actively catches contract violations in the changed conformance surface.

## Limits

`cargo deny check` still fails on the repository's existing unratified license policy surface, and `cargo vet` still fails because `supply-chain/` is not initialized. Those are owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md` and were not introduced by this slice.

`cargo bloat -p cdf-conformance --release --crates` is not applicable because `cdf-conformance` is library-only. `tokei` and `scc` were not installed. No fuzz, Kani, Loom, Criterion, benchmark, or profiler harness is configured for this repository slice, and this change does not touch production unsafe code or performance-sensitive binaries.

This evidence covers the checkpoint-store conformance child only. Resource conformance, destination conformance, chaos killpoints, golden fixtures, and full parent-plan closure remain outside this child ticket.
