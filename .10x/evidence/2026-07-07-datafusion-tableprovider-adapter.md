Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-datafusion-tableprovider-adapter.md, .10x/decisions/datafusion-tier-b-delegation-boundary.md, .10x/specs/resource-authoring-planning-batches.md

# DataFusion TableProvider adapter

## What was observed

`cdf-engine` now exposes an internal `QueryableResourceTableProvider` in `crates/cdf-engine/src/table_provider.rs`, re-exported from the thin `crates/cdf-engine/src/lib.rs`.

The implementation accepts an `Arc<dyn QueryableResource + Send + Sync>` and a `ScopeKey`, translates only simple column/literal binary predicates into CDF `ScanPredicate` values, delegates classification and scan planning to `QueryableResource::negotiate`, maps `Exact` / `Inexact` / `Unsupported` to DataFusion `TableProviderFilterPushDown`, executes negotiated partitions through a real DataFusion `ExecutionPlan`, applies projection to emitted Arrow batches, and suppresses source-side limit pushdown when any inexact pushed filter is present.

`crates/cdf-engine/src/tests.rs` adds focused tests for:

- direct `TableProvider::supports_filters_pushdown` delegation to `QueryableResource::negotiate`
- exact, inexact, unsupported, and unsupported-expression pushdown classification
- registered table/provider scan execution through `TableProvider::scan` and `ExecutionPlan::execute`
- projection over the DataFusion execution output
- disabling limit pushdown for inexact filters while preserving it for exact filters

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

- `cargo test -p cdf-engine --locked`: passed, 17 tests.
- `cargo clippy -p cdf-cli -p cdf-engine --all-targets --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo check --workspace --all-targets --all-features --locked`: passed.
- `cargo check --workspace --all-targets --no-default-features --locked`: passed.
- `cargo nextest run --workspace --locked --no-fail-fast`: passed, 418 tests.
- `cargo hack check --workspace --all-targets --feature-powerset --locked`: passed.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed, 0 doc tests.
- `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked`: passed.
- `tools/codeql-rust-quality.sh`: passed using reusable database path `target/quality/codeql-db-rust`; SARIF result count checked with `jq '[.runs[].results // [] | length] | add' target/quality/reports/codeql-rust-current.sarif` and returned `0`.
- `rg -n "\bunsafe\b|extern \"|unsafe impl|Send for|Sync for|from_raw|into_raw|\*const|\*mut" crates --glob '*.rs'`: found only literal string/test mentions of `unsafe`, not Rust unsafe blocks, FFI declarations, raw pointer use, or unsafe impls.

## What this supports

This supports closing `.10x/tickets/done/2026-07-07-datafusion-tableprovider-adapter.md` for the first generic DataFusion adapter slice:

- the adapter uses DataFusion `TableProvider` and `ExecutionPlan` APIs rather than only DataFusion-shaped metadata
- CDF kernel/resource-authoring APIs remain Arrow-only and do not expose DataFusion types
- pushdown classification remains delegated to `QueryableResource::negotiate`
- simple predicate translation is intentionally narrow and unsupported expressions remain unsupported/residual at the DataFusion boundary
- inexact filters prevent unsafe source-side limit pushdown

## Limits

The proof uses direct `TableProvider::scan` and `ExecutionPlan::execute` collection rather than `DataFrame::collect`, because adding a Tokio test runtime dependency only for this assertion would widen the implementation slice. Residual filter application after DataFusion optimizer planning remains a future integration-depth target when the package execution path is replaced by DataFusion physical execution.
