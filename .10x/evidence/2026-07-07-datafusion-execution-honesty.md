Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-datafusion-execution-honesty.md, .10x/decisions/datafusion-tier-b-delegation-boundary.md, .10x/research/2026-07-07-datafusion-delegation-pushdown-triage.md

# DataFusion execution honesty

## What was observed

Current engine operator metadata now serializes the CDF-native execution path as `cdf_resource_adapter` followed by `cdf_native_scan`, with `adapter_kind` set to `cdf_native_resource_adapter`. The current plan/explain JSON no longer serializes `data_fusion_table_provider`, `data_fusion_scan_exec`, or `datafusion_table_provider` for CDF-native execution.

The DataFusion pushdown fidelity mapping remains in place through `datafusion_filter_pushdown`, and the existing Tier B planner test still asserts `PushdownFidelity::Exact` maps to `TableProviderFilterPushDown::Exact`.

The live local-file conformance fixture was updated to use the honest operator metadata. Its expected package evidence changed only for the package hash/signing input and `plan/explain.json` byte/hash values.

## Procedure

- `cargo fmt --all -- --check` passed before and after fixture updates.
- `cargo test -p cdf-engine --locked --no-fail-fast` first failed on a stale public re-export of `DATAFUSION_TABLE_PROVIDER_KIND`; after renaming the export to `CDF_NATIVE_RESOURCE_ADAPTER_KIND`, it passed with 12 tests.
- `cargo test -p cdf-conformance --locked --no-fail-fast` passed with 40 tests, including the live local-file 100-run golden check.
- `cargo clippy -p cdf-engine --all-targets --locked -- -D warnings` passed.
- `git diff --check` passed.
- `gitleaks protect --no-banner --redact --verbose` passed with no leaks found in the current diff.
- `gitleaks detect --source . --no-banner --redact` failed on 2 findings in full repository history. This is broader than the ticket diff and was not introduced by this change; no secret values were recorded here.

Parent verification after worker integration:

- `cargo fmt --all -- --check` passed.
- `git diff --check -- . ':(exclude).gitignore'` passed.
- `rg -n "data_fusion_table_provider|data_fusion_scan_exec|datafusion_table_provider" crates/cdf-engine crates/cdf-conformance/golden crates/cdf-conformance/src` returned only the intentional negative assertions in `crates/cdf-engine/src/tests.rs`.
- `python3 -m json.tool crates/cdf-conformance/golden/live-local-file-v1/expected.json >/dev/null` passed.
- `cargo test -p cdf-engine -p cdf-conformance --locked --no-fail-fast` passed: `cdf-conformance` reported 40 tests and 0 doctests; `cdf-engine` reported 12 tests and 0 doctests. The conformance run included the live local-file 100-run golden check.
- `cargo clippy -p cdf-engine -p cdf-conformance --all-targets --locked -- -D warnings` passed.
- Focused unsafe/FFI marker scan over `crates/cdf-engine/src`, `crates/cdf-conformance/src`, and `crates/cdf-conformance/golden/live-local-file-v1` returned no matches.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-datafusion-honesty.json crates/cdf-engine crates/cdf-conformance` passed with 0 findings.
- `cargo check --workspace --all-targets --locked` passed.

CodeQL was skipped under the active goal instruction.

## What this supports or challenges

This supports the ticket criteria that current CDF-native plans no longer claim real DataFusion `TableProvider` or physical execution nodes, while explain output still carries pushed, inexact, unsupported, partition, estimate, boundedness, and delivery-guarantee details.

This also supports that the change is metadata honesty only: no dependency changes, no real DataFusion adapter implementation, no predicate-language expansion, and no source/destination behavior changes were introduced.

## Limits

This evidence does not prove a future DataFusion `TableProvider` adapter. That remains blocked by the Arrow/DataFusion dependency tuple alignment owner.

The full-history `gitleaks detect` finding is unresolved outside this ticket. The diff-scoped scan is the evidence relevant to this change.
