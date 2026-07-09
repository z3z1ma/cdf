use cdf_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
use cdf_engine::{EnginePlan, EnginePlanInput, PlanBoundedness, Planner};
use cdf_kernel::{QueryableResource, Result, ScanRequest};
use serde_json::json;

use super::{MatrixDisposition, file_fixture};

pub(crate) fn file_engine_plan(
    package_id: &str,
    disposition: MatrixDisposition,
) -> Result<EnginePlan> {
    serde_json::from_value(file_engine_plan_json(package_id, disposition)).map_err(|error| {
        cdf_kernel::CdfError::data(format!("build run matrix engine plan: {error}"))
    })
}

pub(crate) fn planned_engine_plan<R>(resource: &R, package_id: &str) -> Result<EnginePlan>
where
    R: QueryableResource + ?Sized,
{
    let observed_schema = ObservedSchema::from_arrow(resource.schema().as_ref());
    let policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    let validation_program = compile_validation_program(&policy, &observed_schema)?;
    Planner::new().plan_tier_b(
        resource,
        EnginePlanInput {
            request: ScanRequest {
                resource_id: resource.descriptor().resource_id.clone(),
                projection: None,
                filters: Vec::new(),
                limit: None,
                order_by: Vec::new(),
                scope: resource.descriptor().state_scope.clone(),
            },
            validation_program,
            boundedness: PlanBoundedness::Bounded,
            package_id: package_id.to_owned(),
        },
    )
}

fn file_engine_plan_json(package_id: &str, disposition: MatrixDisposition) -> serde_json::Value {
    let delivery_guarantee = match disposition {
        MatrixDisposition::Append => "effectively_once_per_package",
        MatrixDisposition::Replace => "effectively_once_per_target",
        MatrixDisposition::Merge => "effectively_once_per_key",
    };
    let validation_program = file_validation_program_json();
    let scope = json!({ "kind": "file", "path": file_fixture::SOURCE_POSITION_PATH });
    let scan = json!({
        "plan_id": format!("plan-{}", file_fixture::RESOURCE_ID),
        "request": {
            "resource_id": file_fixture::RESOURCE_ID,
            "projection": null,
            "filters": [],
            "limit": null,
            "order_by": [],
            "scope": scope,
        },
        "partitions": [{
            "partition_id": "files",
            "scope": scope,
            "start_position": null,
            "metadata": {
                "bytes": file_fixture::SOURCE_SIZE_BYTES.to_string(),
                "kind": "files",
                "glob": file_fixture::SOURCE_POSITION_PATH,
                "path": file_fixture::SOURCE_POSITION_PATH,
                "resource_id": file_fixture::RESOURCE_ID,
                "sha256": file_fixture::SOURCE_SHA256,
            },
        }],
        "pushed_predicates": [],
        "unsupported_predicates": [],
        "estimated_rows": null,
        "estimated_bytes": null,
        "delivery_guarantee": delivery_guarantee,
    });
    let operator_chain = json!([
        {
            "kind": "cdf_resource_adapter",
            "adapter_kind": "cdf_native_resource_adapter",
            "resource_id": file_fixture::RESOURCE_ID,
        },
        {
            "kind": "cdf_native_scan",
            "projection": null,
            "residual_predicates": [],
            "limit": null,
        },
        { "kind": "schema_fingerprint_exec" },
        {
            "kind": "contract_exec",
            "normalizer_version": "namecase-v1",
            "column_program_count": 2,
        },
        {
            "kind": "normalize_exec",
            "normalizer_version": "namecase-v1",
        },
        { "kind": "profile_exec" },
        { "kind": "lineage_exec" },
        {
            "kind": "package_sink",
            "package_id": package_id,
        },
    ]);

    json!({
        "scan": scan,
        "final_projection": null,
        "residual_predicates": [],
        "boundedness": { "kind": "bounded" },
        "validation_program": validation_program,
        "operator_chain": operator_chain,
        "explain": {
            "resource_id": file_fixture::RESOURCE_ID,
            "projected_fields": [],
            "projection_pushed": false,
            "limit": null,
            "limit_pushed": false,
            "pushed_predicates": [],
            "inexact_predicates": [],
            "unsupported_predicates": [],
            "partitions": [{
                "partition_id": "files",
                "scope_kind": "file",
                "metadata": {
                    "bytes": file_fixture::SOURCE_SIZE_BYTES.to_string(),
                    "kind": "files",
                    "glob": file_fixture::SOURCE_POSITION_PATH,
                    "path": file_fixture::SOURCE_POSITION_PATH,
                    "resource_id": file_fixture::RESOURCE_ID,
                    "sha256": file_fixture::SOURCE_SHA256,
                },
            }],
            "estimates": {
                "support": "bytes",
                "rows": null,
                "bytes": null,
            },
            "delivery_guarantee": delivery_guarantee,
            "boundedness": { "kind": "bounded" },
            "operator_chain": operator_chain,
        },
        "package_id": package_id,
    })
}

fn file_validation_program_json() -> serde_json::Value {
    json!({
        "normalizer_version": "namecase-v1",
        "schema_verdicts": [],
        "column_programs": [
            {
                "source_name": "id",
                "output_name": "id",
                "arrow_type": { "kind": "int", "signed": true, "bits": 64 },
                "steps": [],
                "nested_action": { "kind": "not_nested" },
                "redaction": { "kind": "preserve" },
            },
            {
                "source_name": "name",
                "output_name": "name",
                "arrow_type": { "kind": "utf8" },
                "steps": [],
                "nested_action": { "kind": "not_nested" },
                "redaction": { "kind": "preserve" },
            },
        ],
        "row_dispositions": [
            { "outcome": "pass", "disposition": "accept" },
            { "outcome": "coerced", "disposition": "accept" },
            { "outcome": "admitted_as_variant", "disposition": "accept" },
            { "outcome": "violation", "disposition": "quarantine" },
            { "outcome": "fatal", "disposition": "reject_run" },
        ],
        "transforms": [],
        "promotion": {
            "clean_runs_required": 3,
            "allow_sampled_fast_path": false,
            "demote_on_drift": true,
            "demote_on_anomaly": true,
            "demote_on_quarantine": true,
        },
        "warnings": [],
    })
}
