use super::MatrixDisposition;
use cdf_contract::{ContractPolicy, IdentifierPolicy, ObservedSchema, compile_validation_program};
use cdf_engine::{EnginePlan, EnginePlanInput, Planner};
use cdf_kernel::ExecutionExtent;
use cdf_kernel::{QueryableResource, Result, ScanRequest};

pub(crate) fn file_engine_plan<R>(
    resource: &R,
    package_id: &str,
    disposition: MatrixDisposition,
    identifier_policy: Option<&IdentifierPolicy>,
) -> Result<EnginePlan>
where
    R: QueryableResource + ?Sized,
{
    if resource.descriptor().write_disposition != disposition.to_write_disposition() {
        return Err(cdf_kernel::CdfError::contract(
            "run-matrix disposition does not match compiled resource",
        ));
    }
    planned_engine_plan(resource, package_id, identifier_policy)
}

pub(crate) fn planned_engine_plan<R>(
    resource: &R,
    package_id: &str,
    identifier_policy: Option<&IdentifierPolicy>,
) -> Result<EnginePlan>
where
    R: QueryableResource + ?Sized,
{
    planned_engine_plan_with_limit(resource, package_id, identifier_policy, None)
}

pub(crate) fn planned_engine_plan_with_limit<R>(
    resource: &R,
    package_id: &str,
    identifier_policy: Option<&IdentifierPolicy>,
    limit: Option<u64>,
) -> Result<EnginePlan>
where
    R: QueryableResource + ?Sized,
{
    let observed_schema = ObservedSchema::from_arrow(resource.schema().as_ref());
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    if let Some(identifier_policy) = identifier_policy {
        policy.normalization.identifier = identifier_policy.clone();
    }
    let validation_program = compile_validation_program(&policy, &observed_schema)?;
    Planner::new().plan_tier_b(
        resource,
        EnginePlanInput {
            request: ScanRequest {
                resource_id: resource.descriptor().resource_id.clone(),
                projection: None,
                filters: Vec::new(),
                limit,
                order_by: Vec::new(),
                scope: resource.descriptor().state_scope.clone(),
            },
            validation_program,
            execution_extent: ExecutionExtent::bounded(),
            keyed_effects: cdf_kernel::KeyedEffectPlanAuthority::deletes_unsupported(),
            segmentation: cdf_engine::CanonicalSegmentationPolicy::performance_default(),
            package_id: package_id.to_owned(),
            relational_expression_plan: None,
            committed_frontier: None,
        },
    )
}
