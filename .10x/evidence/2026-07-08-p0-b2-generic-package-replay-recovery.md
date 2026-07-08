Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-07-p0-b2-generic-package-replay-recovery.md, .10x/tickets/2026-07-07-p0-workstream-b-open-orchestrator-world.md, .10x/decisions/project-destination-driver-registry.md

# P0 B2 Generic Package Replay and Recovery

## What was observed

`cdf-project` package replay and recovery now have generic skeletons:

- `replay_package_with_runtime(...)`
- `recover_package_with_runtime(...)`

The skeletons operate over `ProjectDestinationRuntime`, kernel `DestinationProtocol`, and segment-writing `CommitSession`.

Existing DuckDB, Parquet, and Postgres public compatibility wrappers remain for B4, but their replay/recovery commit gates now delegate through project destination runtime adapters instead of owning separate destination-specific commit/checkpoint flows.

The implementation added:

- `ProjectDestinationRegistry` with driver registration, duplicate scheme rejection, and URI-scheme resolution.
- DuckDB, Parquet, and Postgres `ProjectDestinationRuntime` adapters.
- generic package replay stages for package-verified, checkpoint-proposed, destination-write-ready, destination-commit-started, destination-receipt-recorded, checkpoint-committed, and package-status-updated.
- trait-level receipt verification through `verify_destination_receipt_before_checkpoint(&dyn DestinationProtocol, ...)`.
- mock driver/registry tests proving a registered destination can resolve to a runtime and drive generic replay, recovery, and generic stop-before-destination-write hook injection without generic replay/orchestrator edits.

The parent review found one closure gap in the first worker patch: the mock test constructed a runtime directly rather than resolving a registered destination. The repair added `ProjectDestinationRegistry` and converted the mock tests to register and resolve `MockProjectDestinationDriver`.

## Procedure

Parent-observed checks:

- `cargo fmt --check`: passed.
- `git diff --check`: passed.
- `cargo check -p cdf-project --all-targets`: passed.
- `cargo clippy -p cdf-project --all-targets -- -D warnings`: passed.
- `cargo test -p cdf-project --no-fail-fast`: passed, 64 unit tests and 0 doc-tests.
- `cargo nextest run -p cdf-project --locked`: passed, 64 tests.
- `cargo hack check -p cdf-project --all-targets --each-feature --locked`: passed.
- `cargo semver-checks -p cdf-project --baseline-rev HEAD`: passed, 196 checks passed and 57 skipped.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-p0-b2-cdf-project.json crates/cdf-project/src`: passed with 0 findings across 19 tracked files.
- `gitleaks dir` over a bounded snapshot of the touched runtime source and records: passed with no leaks; report at `target/quality/reports/gitleaks-p0-b2-touched.json`.
- direct unsafe/soundness source scan over `crates/cdf-project/src/runtime.rs`, `crates/cdf-project/src/runtime/*.rs`, and `crates/cdf-project/src/runtime_tests.rs`: found no `unsafe`, extern blocks, raw pointers, transmute, or manual `Send`/`Sync` impls. Hits were `Box<dyn Any + Send + Sync>` in the B1 pending-context surface and test-only `Arc`, `Mutex`, and `Atomic*` usage.

Quality metrics:

- `jscpd --min-lines 8 --min-tokens 80 --threshold 10 --reporters console crates/cdf-project/src/runtime.rs crates/cdf-project/src/runtime crates/cdf-project/src/runtime_tests.rs`: passed with 16 clones, 293 duplicated lines, 1,892 duplicated tokens, 4.11% duplicated lines, and 4.45% duplicated tokens.
- `scc crates/cdf-project/src/runtime.rs crates/cdf-project/src/runtime crates/cdf-project/src/runtime_tests.rs`: 12 Rust files, 7,127 lines, 6,708 code lines, aggregate complexity 191.
- `rust-code-analysis-cli` reports were written to `target/quality/reports/rca-p0-b2-runtime.json` and `target/quality/reports/rca-p0-b2-runtime-tests.json`.
- Rust-code-analysis runtime hotspots: `runtime/replay.rs` module cyclomatic 164/cognitive 39; `runtime/destinations.rs` module 116/18; `runtime/artifacts.rs` module 97/33; `runtime/orchestration.rs` module 81/7; new `replay_package_with_runtime` function 19/6.
- Rust-code-analysis test hotspot: `runtime_tests.rs` unit cyclomatic 241/cognitive 32; highest new mock impl was `MockCommitSession` at 15/4.

Supply-chain and security:

- `cargo deny check`: passed; duplicate-version warnings remain the already-ratified Arrow 58/59 residual.
- `cargo audit`: passed with one allowed warning, `RUSTSEC-2024-0436` for `paste 1.0.15`.
- `cargo vet --locked`: passed, `Vetting Succeeded (393 exempted)`.
- `osv-scanner scan source --lockfile Cargo.lock --format json --output target/quality/reports/osv-p0-b2.json .`: exited nonzero with exactly one result, `RUSTSEC-2024-0436` for `paste 1.0.15`, matching the active scoped exception.
- bare `cargo vet`: failed with the DataFusion git pin `policy.audit-as-crates-io` issue now owned by `.10x/tickets/2026-07-08-cargo-vet-datafusion-git-policy-bare-command.md`. This is not a B2 runtime defect, and the locked vet gate passed.
- `tools/codeql-rust-quality.sh`: passed using the reusable database path `target/quality/codeql-db-rust`. It refreshed the database because Rust source changed after the registry repair, then analyzed with 0 SARIF results. Extractor diagnostics were the known local Rust macro-expansion warning noise: 186 Rust files scanned, 0 extraction errors, 2,879 extraction warnings, 142 files with warnings, and 44 without warnings.

## What this supports

This supports B2 acceptance:

- one generic replay path handles package verification, checkpoint proposal, loading status, destination commit session, receipt identity and trait-level verification, checkpoint commit, and package checkpointed status;
- one generic recovery path handles durable receipt validation, trait-level verification, checkpoint commit/reuse, and package checkpointed status without destination mutation;
- DuckDB, Parquet, and Postgres wrappers now delegate through project destination runtime adapters;
- generic stage hooks cover the currently ratified crash windows, with DuckDB failpoint names preserved only as compatibility adapters;
- the mock registered destination test proves replay, recovery, and generic failpoint injection through a registered driver/runtime adapter.

## Limits

B2 does not delete public DuckDB/Parquet/Postgres compatibility wrappers, migrate CLI/conformance callers, or remove closed run resource/destination enums. B3 and B4 own those steps.

The new registry is intentionally minimal. B3 owns built-in project/URI resolution for current production destinations and resource construction.

The bare `cargo vet` residual remains open under `.10x/tickets/2026-07-08-cargo-vet-datafusion-git-policy-bare-command.md`.
