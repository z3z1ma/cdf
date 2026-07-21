use cdf_contract::{ContractPolicy, ObservedSchema, compile_resource_validation_program};
use cdf_engine::{EnginePlan, EnginePlanInput, Planner};
use cdf_kernel::ExecutionExtent;
use cdf_kernel::{
    CdfError, CheckpointId, CursorOrderingClaim, IncrementalShape, PipelineId, PredicateId,
    PushdownFidelity, QueryableResource, Result, ScanPredicate, ScanRequest, ScopeKey, TargetName,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::runtime::WindowScopedResource;

pub const BACKFILL_PIPELINE_ID: &str = "cdf-backfill";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackfillPlanRequest {
    pub target: TargetName,
    pub from: String,
    pub to: String,
    pub slice_size: Option<u64>,
    pub segmentation: cdf_engine::CanonicalSegmentationPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BackfillPlan {
    pub resource_id: String,
    pub target: String,
    pub from: String,
    pub to: String,
    pub slices: Vec<BackfillSlice>,
    pub pipeline_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BackfillSlice {
    pub ordinal: usize,
    pub start: String,
    pub end: String,
    pub filters: Vec<String>,
    pub package_id: String,
    pub checkpoint_id: String,
    pub scope: ScopeKey,
    #[serde(skip)]
    pub engine_plan: EnginePlan,
}

impl BackfillSlice {
    pub fn checkpoint_id(&self) -> Result<CheckpointId> {
        CheckpointId::new(self.checkpoint_id.clone())
    }
}

pub fn plan_backfill(
    resource: &dyn QueryableResource,
    source_plan: &cdf_runtime::CompiledSourcePlan,
    request: BackfillPlanRequest,
) -> Result<BackfillPlan> {
    validate_backfill_eligible(resource)?;
    let slices = requested_slices(&request)?;
    let target = request.target.clone();
    let cursor = resource
        .descriptor()
        .cursor
        .as_ref()
        .expect("validate_backfill_eligible requires cursor");
    let validation_program = compile_resource_validation_program(
        &ContractPolicy::for_trust(resource.descriptor().trust_level.clone()),
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
        resource.descriptor(),
    )?;
    let planner = Planner::new();
    let mut planned = Vec::with_capacity(slices.len());
    for (index, (start, end)) in slices.into_iter().enumerate() {
        let scope = ScopeKey::Window {
            start: start.clone(),
            end: end.clone(),
        };
        let scoped = WindowScopedResource::new(resource, scope.clone());
        let filters = vec![
            format!("{} >= {}", cursor.field, predicate_literal(&start)),
            format!("{} < {}", cursor.field, predicate_literal(&end)),
        ];
        let package_id = deterministic_id("cdf-backfill-pkg", resource, &target, &start, &end);
        let checkpoint_id = deterministic_id("cdf-backfill-cp", resource, &target, &start, &end);
        let scan_request = ScanRequest {
            resource_id: resource.descriptor().resource_id.clone(),
            projection: None,
            filters: filters
                .iter()
                .enumerate()
                .map(|(filter_index, expression)| {
                    ScanPredicate::new(
                        PredicateId::new(format!("backfill-window-{:03}", filter_index + 1))?,
                        expression.clone(),
                    )
                })
                .collect::<Result<Vec<_>>>()?,
            limit: None,
            order_by: Vec::new(),
            scope: scope.clone(),
        };
        let engine_plan = planner.plan_tier_b(
            &scoped,
            EnginePlanInput {
                request: scan_request,
                validation_program: validation_program.clone(),
                execution_extent: ExecutionExtent::bounded(),
                segmentation: request.segmentation.clone(),
                package_id: package_id.clone(),
            },
        )?;
        let engine_plan = engine_plan.bind_compiled_source(source_plan)?;
        validate_exact_cursor_window(&engine_plan)?;
        planned.push(BackfillSlice {
            ordinal: index + 1,
            start,
            end,
            filters,
            package_id,
            checkpoint_id,
            scope,
            engine_plan,
        });
    }

    Ok(BackfillPlan {
        resource_id: resource.descriptor().resource_id.to_string(),
        target: target.to_string(),
        from: request.from,
        to: request.to,
        slices: planned,
        pipeline_id: BACKFILL_PIPELINE_ID.to_owned(),
    })
}

pub fn backfill_pipeline_id() -> Result<PipelineId> {
    PipelineId::new(BACKFILL_PIPELINE_ID)
}

fn validate_backfill_eligible(resource: &dyn QueryableResource) -> Result<()> {
    let descriptor = resource.descriptor();
    let Some(cursor) = &descriptor.cursor else {
        return Err(CdfError::contract(format!(
            "resource `{}` is not eligible for backfill: no cursor is declared",
            descriptor.resource_id
        )));
    };
    if cursor.ordering == CursorOrderingClaim::Unordered {
        return Err(CdfError::contract(format!(
            "resource `{}` is not eligible for backfill: cursor `{}` is unordered",
            descriptor.resource_id, cursor.field
        )));
    }
    if resource.capabilities().incremental != IncrementalShape::Cursor {
        return Err(CdfError::contract(format!(
            "resource `{}` is not eligible for backfill: expected a cursor-backed queryable resource, got {:?}",
            descriptor.resource_id,
            resource.capabilities().incremental
        )));
    }
    Ok(())
}

fn requested_slices(request: &BackfillPlanRequest) -> Result<Vec<(String, String)>> {
    let Some(slice_size) = request.slice_size else {
        if let (Ok(from), Ok(to)) = (request.from.parse::<u64>(), request.to.parse::<u64>())
            && from >= to
        {
            return Err(CdfError::contract(
                "backfill range must satisfy --from < --to for numeric cursor bounds",
            ));
        }
        return Ok(vec![(request.from.clone(), request.to.clone())]);
    };
    if slice_size == 0 {
        return Err(CdfError::contract("--slice-size must be greater than zero"));
    }
    let from = request.from.parse::<u64>().map_err(|error| {
        CdfError::contract(format!(
            "--from must be a non-negative integer when --slice-size is used: {error}"
        ))
    })?;
    let to = request.to.parse::<u64>().map_err(|error| {
        CdfError::contract(format!(
            "--to must be a non-negative integer when --slice-size is used: {error}"
        ))
    })?;
    if from >= to {
        return Err(CdfError::contract(
            "backfill range must satisfy --from < --to when --slice-size is used",
        ));
    }

    let mut slices = Vec::new();
    let mut start = from;
    while start < to {
        let end = start.saturating_add(slice_size).min(to);
        slices.push((start.to_string(), end.to_string()));
        start = end;
    }
    Ok(slices)
}

fn validate_exact_cursor_window(plan: &EnginePlan) -> Result<()> {
    if !plan.residual_predicates.is_empty()
        || plan.scan.pushed_predicates.len() != plan.scan.request.filters.len()
        || plan
            .scan
            .pushed_predicates
            .iter()
            .any(|predicate| predicate.fidelity != PushdownFidelity::Exact)
    {
        let filters = plan
            .scan
            .request
            .filters
            .iter()
            .map(|predicate| predicate.expression.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(CdfError::contract(format!(
            "resource `{}` is not eligible for backfill: cursor window predicates must be pushed exactly before execution: {filters}",
            plan.scan.request.resource_id
        )));
    }
    Ok(())
}

fn predicate_literal(value: &str) -> String {
    if value.parse::<u64>().is_ok() {
        value.to_owned()
    } else {
        format!("'{}'", value.replace('\'', "''"))
    }
}

fn deterministic_id(
    prefix: &str,
    resource: &dyn QueryableResource,
    target: &TargetName,
    start: &str,
    end: &str,
) -> String {
    let mut hash = Sha256::new();
    hash.update(resource.descriptor().resource_id.as_str().as_bytes());
    hash.update(b"\0");
    hash.update(target.as_str().as_bytes());
    hash.update(b"\0");
    hash.update(start.as_bytes());
    hash.update(b"\0");
    hash.update(end.as_bytes());
    let digest = hex::encode(hash.finalize());
    format!("{prefix}-{}", &digest[..16])
}
