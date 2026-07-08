Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-p0-b1-runtime-registry-foundation.md, .10x/tickets/2026-07-07-p0-workstream-b-open-orchestrator-world.md, .10x/decisions/project-destination-driver-registry.md

# P0 B1 Runtime Registry Foundation

## What was observed

`crates/cdf-project/src/runtime.rs` was split from a monolithic implementation file into a facade over focused runtime modules. Existing public run/replay/recover APIs remain re-exported for downstream B2-B4 migration, while the project destination registry foundation types required by `.10x/decisions/project-destination-driver-registry.md` now exist.

Before B1, `crates/cdf-project/src/runtime.rs` had 2,931 lines and owned public runtime DTOs, run orchestration, validation, state-delta/artifact construction, ledger event recording, replay/recovery, failpoints, receipt validation, and destination-specific helper code in one module.

After B1, module shape is:

- `crates/cdf-project/src/runtime.rs`: 77-line facade and shared prelude.
- `crates/cdf-project/src/runtime/artifacts.rs`: 474 lines, state-delta and package state/commit artifacts.
- `crates/cdf-project/src/runtime/destinations.rs`: 283 lines, project destination foundation API and current destination-specific planning helpers.
- `crates/cdf-project/src/runtime/hooks.rs`: 22 lines, receipt and runtime stage hooks.
- `crates/cdf-project/src/runtime/ledger.rs`: 153 lines, run ledger recorder.
- `crates/cdf-project/src/runtime/orchestration.rs`: 443 lines, current run orchestration and compatibility run wrappers.
- `crates/cdf-project/src/runtime/receipts.rs`: 148 lines, receipt identity and trait-level verification helpers.
- `crates/cdf-project/src/runtime/replay.rs`: 1,076 lines, current replay/recovery and compatibility wrappers.
- `crates/cdf-project/src/runtime/resources.rs`: 42 lines, current resource adapter enum.
- `crates/cdf-project/src/runtime/types.rs`: 255 lines, public request/report DTOs.
- `crates/cdf-project/src/runtime/validation.rs`: 210 lines, current runtime validation helpers.

New public foundation API:

- `ProjectDestinationDriver`
- `ProjectDestinationRuntime`
- `PreparedDestinationCommit`
- `ProjectDestinationDescription`
- `DestinationReceiptReportingPolicy`
- `ProjectResolutionContext`
- `DestinationPlanningContext`
- `RuntimeStage`
- `RuntimeStageHook`

`PreparedDestinationCommit` also exposes `new`, `with_pending_context`, `take_pending_context`, and `has_pending_context`, and `ProjectDestinationRuntime` requires `bind_prepared_commit(&mut PreparedDestinationCommit)`. This makes the ratified pending-context handoff usable by B2: an adapter can bind its package-aware pending context before generic code calls `protocol().begin(commit, plan)`.

Existing public compatibility APIs remain visible in the runtime facade, including `run_project`, `run_local_file_to_duckdb_checkpoint`, DuckDB/Parquet/Postgres artifact replay/recovery wrappers, DuckDB prepared replay/recovery wrappers, and the existing request/report DTOs. No CLI, conformance, or destination crate files were edited.

## Procedure

Parent-observed verification commands:

- `cargo fmt --check` passed.
- `git diff --check` passed.
- `cargo check -p cdf-project --all-targets` passed.
- `cargo test -p cdf-project --no-fail-fast` passed with 62 unit tests and 0 doc-tests.
- `cargo clippy -p cdf-project --all-targets -- -D warnings` passed.
- `cargo semver-checks -p cdf-project --baseline-rev HEAD` passed: 196 checks passed, 57 skipped, no semver update required.
- `cargo hack check -p cdf-project --all-targets --each-feature --locked` passed.
- Direct unsafe/soundness surface scan over `crates/cdf-project/src/runtime.rs` and `crates/cdf-project/src/runtime/*.rs` found no `unsafe`, extern, raw pointer, or impl-safety surface. The only match was the intentional `Box<dyn Any + Send + Sync>` field in `PreparedDestinationCommit`.
- Public API inventory command ran: `rg -n "^pub (type|struct|enum|trait|fn)|^pub use" crates/cdf-project/src/runtime.rs crates/cdf-project/src/runtime/*.rs`.
- `jscpd --min-lines 8 --min-tokens 80 --threshold 100 --reporters console crates/cdf-project/src/runtime.rs crates/cdf-project/src/runtime crates/cdf-project/src/runtime_tests.rs` passed. Before B1, the same focused runtime/runtime-tests scope reported 20 clones, 385 duplicated lines, 2,355 duplicated tokens, 6.19% duplicated lines, and 6.32% duplicated tokens. After B1, it reported 20 clones, 383 duplicated lines, 2,355 duplicated tokens, 5.96% duplicated lines, and 6.12% duplicated tokens.
- `rust-code-analysis-cli` before B1 reported one `runtime.rs` unit with cyclomatic 546, cognitive 135, and lloc 525. After B1, the hottest runtime units were `runtime/replay.rs` at cyclomatic 204/cognitive 52, `runtime/artifacts.rs` at 97/33, and `runtime/orchestration.rs` at 81/7.
- `scc crates/cdf-project/src/runtime.rs crates/cdf-project/src/runtime crates/cdf-project/src/runtime_tests.rs` reported 12 Rust files, 6,428 lines, 6,045 code lines, and aggregate complexity 185.

CodeQL was not run for B1. The user explicitly directed that the reusable CodeQL DB should not be recreated casually, and this slice changed no dependencies, unsafe code, security boundary, or dataflow behavior. The relevant local gates for this mechanical module split were formatting, compile, tests, clippy, semver, feature check, duplication, and complexity metrics.

Supply-chain gates (`cargo deny`, `cargo audit`, `cargo vet`, OSV) were not rerun for B1 because no manifest, lockfile, dependency, advisory, source policy, or vendored code changed.

## What this supports

This supports closing `.10x/tickets/done/2026-07-07-p0-b1-runtime-registry-foundation.md` after adversarial review:

- `runtime.rs` is now a facade rather than the owner of all runtime concerns.
- Runtime concerns are split into focused modules.
- The project destination driver/runtime foundation API exists.
- Generic runtime stage hooks exist.
- Existing public APIs still compile for downstream migration.
- Focused quality checks passed, including complexity and duplication evidence.

## Limits

B1 intentionally does not complete Workstream B. The old specialized replay/recovery wrappers, closed run destination/resource enums, and duplicated replay/orchestration internals remain for B2-B4 to migrate and delete.

The largest remaining runtime module is `runtime/replay.rs`; this is expected because B2 owns replacing the specialized replay/recovery internals with the generic skeleton.

This evidence does not claim CLI, conformance, golden, or chaos callers route through the generic runtime. B4 owns caller migration and wrapper deletion.
