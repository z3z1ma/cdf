Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-datafusion-execution-honesty.md
Verdict: pass

# DataFusion execution honesty review

## Target

Review of the metadata-honesty change owned by `.10x/tickets/done/2026-07-07-datafusion-execution-honesty.md`.

## Findings

None.

## Assumptions tested

The old serialized DataFusion execution names are guarded by tests, not merely renamed in source. `cdf-engine` tests serialize the full plan/explain JSON for current Tier A and Tier B plans and assert the old `data_fusion_table_provider`, `data_fusion_scan_exec`, and `datafusion_table_provider` names are absent.

The replacement names are honest for current behavior. `cdf_resource_adapter` and `cdf_native_scan` describe the present CDF-native resource/scan loop without implying a real DataFusion `TableProvider` or `ExecutionPlan`.

The DataFusion pushdown vocabulary was not weakened. The existing `datafusion_filter_pushdown` test assertion still maps `Exact` to DataFusion's `TableProviderFilterPushDown::Exact`, and Tier B tests still cover pushed, inexact, unsupported, partition, estimate, and guarantee details.

The conformance fixture hash churn is explained by the serialized `plan/explain.json` metadata change and was verified by the live local-file 100-run golden test.

## Verdict

Pass. The change is confined to operator metadata naming, focused tests, and the expected golden evidence produced by that serialized plan change. It does not implement or fake a DataFusion adapter, change dependencies, or alter source/destination execution behavior.

## Residual risk

Changing serialized operator names changes package identity for packages that include `plan/explain.json`. The golden fixture was updated and verified, but external package compatibility policy is still governed by the broader artifact-versioning work.

Full-history `gitleaks detect` reports two pre-existing findings outside this ticket. The diff-scoped `gitleaks protect` scan found no leaks in this change.
