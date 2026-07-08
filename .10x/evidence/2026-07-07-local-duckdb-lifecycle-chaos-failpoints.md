Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-06-local-duckdb-lifecycle-chaos-failpoints.md, .10x/specs/package-lifecycle-determinism.md, .10x/specs/checkpoint-state-commit-gate.md, .10x/specs/conformance-governance-roadmap.md

# Local DuckDB lifecycle chaos failpoints evidence

## What was observed

`cdf-project` now exposes additive local DuckDB/SQLite lifecycle failpoints for the package/checkpoint crash matrix:

- after packaged before destination write;
- after checkpoint proposal before destination write;
- after durable receipt verification before checkpoint commit;
- after checkpoint commit before package status becomes checkpointed.

The existing `after_receipt_verified` hook remains source-compatible. No-hook replay and recovery paths still call the same public functions and default to no failpoint.

`cdf-conformance` now drives all four named failpoints through the helper-process boundary where durable state matters. The pre-destination cases prove no DuckDB file, `_cdf_loads`, `_cdf_state`, receipt, or committed checkpoint head exists. The post-receipt case still proves durable receipt recovery without source contact or a second destination write. The post-checkpoint/pre-status case proves the committed head and durable receipt are reused to finalize package status without rewriting destination data.

The symlink requested by the user remains in place: `/Users/alexanderbut/code_projects/personal/cdf -> /Users/alexanderbut/code_projects/personal/firn`.

## Procedure

Implementation inspection:

- `crates/cdf-project/src/runtime.rs` adds `LocalDuckDbLifecycleFailpoint`, `LocalDuckDbLifecycleFailpointHook`, no-hook compatible `_with_failpoint` variants, and exact committed-head reuse for recovery after checkpoint commit but before package status update.
- `crates/cdf-project/src/runtime_tests.rs` covers the checkpoint-proposal pre-destination stop, post-checkpoint status-only recovery, and exact committed-head reuse predicates.
- `crates/cdf-conformance/src/package_replay/mod.rs` adds `assert_no_duckdb_destination_write`.
- `crates/cdf-conformance/src/package_replay/tests.rs` selects named failpoints in the helper process and asserts pre-destination, post-receipt, and post-checkpoint recovery contracts.

Focused checks after final test hardening:

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo clippy -p cdf-project -p cdf-conformance --all-targets --locked -- -D warnings`: passed.
- `cargo test -p cdf-project -p cdf-conformance --locked --no-fail-fast`: passed; 35 `cdf-project` tests, 40 `cdf-conformance` tests, and doc tests passed.
- `cargo nextest run -p cdf-project -p cdf-conformance --locked`: passed; 75 tests passed.

Broad `QUALITY.md` gates run before the final test-only mutation hardening, with final focused checks above covering the changed test crate afterward:

- `cargo check --workspace --all-targets --locked`: passed.
- `cargo check --workspace --all-targets --all-features --locked`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast`: passed; all workspace unit/integration tests passed, including live Postgres tests.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed.
- `cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo nextest run --workspace --locked`: passed; 306 tests passed.
- `cargo hack check --workspace --all-targets --each-feature --locked`: passed.
- `cargo semver-checks --workspace --baseline-rev HEAD`: passed; no semver update required for checked crates.

Security, supply-chain, hygiene, and metrics reports are under `target/quality/reports/local-duckdb-lifecycle-chaos-failpoints/`:

- `cargo deny check`: passed; advisories, bans, licenses, and sources all ok.
- `cargo audit --json`: exited 0 with `vulnerability_count: 0`.
- `cargo vet --locked`: passed; `Vetting Succeeded (420 exempted)`.
- `osv-scanner scan source -r . --format json`: exited 1 only for ratified `RUSTSEC-2024-0436` on `paste`.
- `cargo tree --workspace --locked -i paste`: confirms `paste v1.0.15` is present through `parquet v59.0.0`, matching `.10x/decisions/native-arrow-datafusion-parquet-policy.md`.
- `cargo machete`: passed with no unused dependency candidates.
- Semgrep `p/rust` over `crates/cdf-project` and `crates/cdf-conformance`: passed with 0 findings.
- Semgrep `p/security-audit` over `crates/cdf-project` and `crates/cdf-conformance`: passed with 0 findings.
- Source-snapshot `gitleaks dir --redact`: passed with 0 leaks.
- Broad unsafe scan found one safe `Send + Sync` conformance assertion. Strict unsafe/FFI/raw-pointer scan over the touched crates found no matches.
- `rust-code-analysis-cli`, `jscpd`, `tokei`, and `scc` reports were produced for the touched crates. `jscpd` reports 28 existing Rust clones, 306 duplicated lines, 3.05% total duplication; these are metric signals, not blockers for this scoped change.

Mutation testing:

- First runtime mutation run over the new failpoint/recovery functions found 13 mutants: 4 caught, 6 unviable, and 3 missed in `commit_or_reuse_committed_checkpoint`.
- The missed mutants replaced `&&` with `||` in the committed-head reuse predicate. Parent hardening added `recovery_reuses_only_exact_committed_checkpoint_head`, proving that status, current-head marker, exact delta, and exact receipt are each required.
- Final runtime mutation rerun: 13 mutants tested, 7 caught, 6 unviable, 0 missed.
- Conformance package-replay mutation run over the relevant assertions: 4 mutants tested, 4 caught.

Tool limitations and explicit skips:

- CodeQL was skipped for this slice because the active goal says to skip CodeQL for now and the user specifically requested avoiding CodeQL database recreation. The reusable database remains under `target/quality/codeql-db-rust`; no disposable database was created.
- `cargo geiger`, Miri, cargo-careful, fuzzing, Kani, benchmarks, profiling, and binary-size checks were not run for this ticket. The change is safe Rust test/failpoint plumbing over existing DuckDB/SQLite runtime behavior, has no new unsafe/FFI/raw-pointer surface by direct scan, and is covered by focused tests, process-boundary conformance tests, nextest, and bounded mutation.
- `cargo llvm-cov` was not run; mutation testing and the focused/broad test matrix were used as the test-quality oracle for this lifecycle-chaos slice.

## What this supports or challenges

This supports closing `.10x/tickets/done/2026-07-06-local-duckdb-lifecycle-chaos-failpoints.md`. The implementation satisfies the local DuckDB/SQLite lifecycle chaos slice without changing package artifact schema, adding CLI resume/replay, broadening native Parquet policy, or editing `.gitignore`.

The evidence supports the package lifecycle spec's crash matrix and the checkpoint spec's commit-gate invariant: recovery never advances a checkpoint head ahead of durable destination receipt data, and the post-checkpoint/pre-status path reuses only an exact already-committed head.

## Limits

This evidence does not cover Postgres or Parquet lifecycle chaos, generic destination finalization, CLI `resume`, `cdf replay package`, run-ledger default IDs, HTTP/API or SQL source execution, fuzz/property targets, or the full MVP acceptance demo harness. Those remain parent or separate-ticket scope.
