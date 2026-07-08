Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p0-c3-cross-destination-chaos.md, .10x/tickets/2026-07-07-p0-workstream-c-spine-conformance-harness.md

# P0 C3 cross-destination chaos evidence

## What was observed

`cdf-conformance::runtime_chaos` now drives generic package artifact replay through `replay_package_from_artifacts_with_stage_hook` and public `RuntimeStage` values for DuckDB, filesystem Parquet, and Postgres.

The parent-observed focused chaos run executed 12 cases: 3 destinations x 4 ratified crash windows.

Destinations:

- DuckDB
- filesystem Parquet
- Postgres

Crash windows:

- package replay verified before destination write
- checkpoint proposed before destination write, fired at `DestinationWriteReady` to match the existing DuckDB lifecycle failpoint after package status becomes `Loading`
- destination receipt recorded and verified before checkpoint commit
- checkpoint committed before package status checkpointed

For every executed case, the output reported:

- `recovery_without_source_contact: true`
- `checkpoint_not_ahead_of_durable_data: true`
- `receipt_recovery_avoided_second_destination_write: true`
- `duplicate_retry_no_second_destination_write: true`

There were no destination or crash-window exclusions.

## Procedure

Implementation review inspected these files:

- `crates/cdf-conformance/src/runtime_chaos/mod.rs`
- `crates/cdf-conformance/src/runtime_chaos/destinations.rs`
- `crates/cdf-conformance/src/runtime_chaos/fixture.rs`
- `crates/cdf-conformance/src/runtime_chaos/helper.rs`
- `crates/cdf-conformance/src/runtime_chaos/tests.rs`
- `crates/cdf-conformance/src/run_matrix/mod.rs`
- `crates/cdf-conformance/src/lib.rs`

Parent review changed the checkpoint-proposed crash trigger from `RuntimeStage::CheckpointProposed` to `RuntimeStage::DestinationWriteReady` so C3 preserves the existing named failpoint semantics from `.10x/tickets/done/2026-07-06-local-duckdb-lifecycle-chaos-failpoints.md`.

Commands run after that parent review fix:

```text
cargo fmt --all --check
cargo test -p cdf-conformance runtime_chaos -- --nocapture
cargo nextest run --locked -p cdf-conformance runtime_chaos
cargo check -p cdf-conformance -p cdf-project --all-targets --locked
cargo clippy -p cdf-conformance -p cdf-project --all-targets --locked -- -D warnings
git diff --check
jscpd crates/cdf-conformance/src/runtime_chaos --reporters json,console --output target/quality/reports/jscpd-p0-c3-runtime-chaos --ignore "**/target/**,**/.git/**,**/reports/**"
rust-code-analysis-cli -m -O json -p crates/cdf-conformance/src/runtime_chaos > target/quality/reports/rust-code-analysis-p0-c3-runtime-chaos.json
semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-p0-c3-runtime-chaos.json crates/cdf-conformance/src/runtime_chaos crates/cdf-conformance/src/lib.rs crates/cdf-conformance/src/run_matrix/mod.rs
gitleaks dir --redact --no-banner --report-format json --report-path target/quality/reports/gitleaks-p0-c3-runtime-chaos.json crates/cdf-conformance/src/runtime_chaos
cargo deny check
cargo vet --locked
cargo audit --deny warnings --ignore RUSTSEC-2024-0436
```

Additional audit check:

```text
cargo audit --deny warnings
```

That raw audit command failed only on the already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` unmaintained advisory. The explicit-ignore audit command above passed. This is the same scoped exception governed by `.10x/decisions/native-arrow-datafusion-parquet-policy.md` and `.10x/decisions/datafusion-git-pin-arrow59-tuple.md`.

## Results

- `cargo fmt --all --check`: pass.
- `cargo test -p cdf-conformance runtime_chaos -- --nocapture`: pass; 2 tests passed, 0 failed, 41 filtered out. The cross-destination test printed the 12-case `CDF_RUNTIME_CHAOS_OUTPUT`.
- `cargo nextest run --locked -p cdf-conformance runtime_chaos`: pass; 2 tests passed, 41 skipped.
- `cargo check -p cdf-conformance -p cdf-project --all-targets --locked`: pass.
- `cargo clippy -p cdf-conformance -p cdf-project --all-targets --locked -- -D warnings`: pass.
- `git diff --check`: pass.
- `jscpd`: pass; 5 Rust files analyzed, 1082 lines, 6354 tokens, 0 clones, 0.00% duplicated lines/tokens.
- `rust-code-analysis-cli`: report written to `target/quality/reports/rust-code-analysis-p0-c3-runtime-chaos.json`; 5 files, 54 functions. Max function cyclomatic complexity was 17 in `execute_case` at `crates/cdf-conformance/src/runtime_chaos/tests.rs:69`; max function cognitive complexity was 5 in `run_helper_process` at `crates/cdf-conformance/src/runtime_chaos/helper.rs:65`.
- `semgrep scan --config p/rust`: pass; 7 targets scanned, 11 Rust rules, 0 findings.
- `gitleaks dir`: pass; no leaks found in `crates/cdf-conformance/src/runtime_chaos`.
- `cargo deny check`: pass; emitted already-known duplicate Arrow 58/59 warnings covered by Workstream D records.
- `cargo vet --locked`: pass; `Vetting Succeeded (393 exempted)`.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: pass.

CodeQL was not rerun for C3 because this slice changed conformance harness code only, did not change production runtime or dependencies, and `.10x/knowledge/quality-gate-execution.md` says to reuse the expensive database only when CodeQL is needed for the current source/risk. The existing reusable database was not recreated.

## What this supports

This supports closing `.10x/tickets/done/2026-07-08-p0-c3-cross-destination-chaos.md`:

- chaos failpoints are driven through the generic `RuntimeStage` seam;
- DuckDB, filesystem Parquet, and Postgres each cover all four ratified crash windows;
- pre-destination crashes leave no destination write and no checkpoint head;
- durable-receipt crashes recover through package artifact recovery without a second destination write;
- checkpoint-committed crashes finalize package status without advancing beyond the verified durable receipt;
- duplicate retry is idempotent per destination receipt behavior;
- receipt verification is exercised through `DestinationProtocol::verify`.

## Limits

Postgres destination footprint comparison records target, load, and state row counts rather than hashing all target rows. The harness also checks stable receipt identity and trait-level receipt verification, so the residual same-count mutation risk is low for this C3 idempotence claim.

The focused run does not replace C4 per-destination live-run golden fixtures or C5 property/fuzz targets.
