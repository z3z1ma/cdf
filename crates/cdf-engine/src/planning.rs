use std::collections::BTreeSet;

use cdf_contract::{ValidationProgram, assert_verdict_lattice_total};
use cdf_kernel::{
    CapabilitySupport, CdfError, DeliveryGuarantee, EstimateSupport, PartitionPlan, PlanId,
    PushdownFidelity, QueryableResource, ResourceCapabilities, ResourceId, ResourceStream, Result,
    ScanPlan, ScanPredicate, ScanRequest, WriteDisposition,
};

use crate::{
    EnginePlan, EnginePlanInput, EstimateExplain, ExplainData, OperatorNode, PartitionExplain,
    PlanBoundedness, PredicateExplain, predicates::predicate_operator,
};

pub const CDF_NATIVE_RESOURCE_ADAPTER_KIND: &str = "cdf_native_resource_adapter";

#[derive(Debug, Default)]
pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self
    }

    pub fn plan_tier_a<R>(&self, resource: &R, input: EnginePlanInput) -> Result<EnginePlan>
    where
        R: ResourceStream + ?Sized,
    {
        validate_boundedness(&input.boundedness)?;
        validate_program(&input.validation_program)?;

        let partitions = resource.plan_partitions(&input.request)?;
        let scan = ScanPlan {
            plan_id: PlanId::new(format!("plan-{}", input.request.resource_id.as_str()))?,
            request: input.request.clone(),
            partitions,
            pushed_predicates: Vec::new(),
            unsupported_predicates: input.request.filters.clone(),
            estimated_rows: None,
            estimated_bytes: None,
            delivery_guarantee: delivery_guarantee(resource.descriptor().write_disposition.clone()),
        };

        self.finish_plan(scan, input, false, false, EstimateSupport::None)
    }

    pub fn plan_tier_b<R>(&self, resource: &R, input: EnginePlanInput) -> Result<EnginePlan>
    where
        R: QueryableResource + ?Sized,
    {
        validate_boundedness(&input.boundedness)?;
        validate_program(&input.validation_program)?;

        let scan = resource.negotiate(&input.request)?;
        self.finish_plan(
            scan,
            input,
            resource.capabilities().projection == CapabilitySupport::Supported,
            resource.capabilities().limits == CapabilitySupport::Supported,
            resource.capabilities().estimates.clone(),
        )
    }

    fn finish_plan(
        &self,
        scan: ScanPlan,
        input: EnginePlanInput,
        projection_pushed: bool,
        limit_pushed: bool,
        estimate_support: EstimateSupport,
    ) -> Result<EnginePlan> {
        let residual_predicates = residual_predicates(&scan);
        let final_projection = input.request.projection.clone();
        let operator_chain = operator_chain(
            &scan.request.resource_id,
            &final_projection,
            &residual_predicates,
            scan.request.limit,
            &input.validation_program,
            &input.package_id,
        );
        let explain = explain_data(
            &scan,
            &input.boundedness,
            &operator_chain,
            projection_pushed,
            limit_pushed,
            estimate_support,
        );

        Ok(EnginePlan {
            scan,
            final_projection,
            residual_predicates,
            boundedness: input.boundedness,
            validation_program: input.validation_program,
            operator_chain,
            explain,
            package_id: input.package_id,
        })
    }
}

pub fn negotiate_scan_plan(
    resource_id: ResourceId,
    request: ScanRequest,
    capabilities: &ResourceCapabilities,
    partitions: Vec<PartitionPlan>,
    estimated_rows: Option<u64>,
    estimated_bytes: Option<u64>,
    delivery_guarantee: DeliveryGuarantee,
) -> Result<ScanPlan> {
    let mut pushed_predicates = Vec::new();
    let mut unsupported_predicates = Vec::new();
    let supported_operators: BTreeSet<&str> = capabilities
        .filters
        .supported_operators
        .iter()
        .map(String::as_str)
        .collect();

    for predicate in &request.filters {
        let operator = predicate_operator(&predicate.expression);
        let supported = operator
            .as_deref()
            .is_some_and(|operator| supported_operators.contains(operator));
        if supported && capabilities.filters.default_fidelity != PushdownFidelity::Unsupported {
            pushed_predicates.push(cdf_kernel::PushedPredicate {
                predicate: predicate.clone(),
                fidelity: capabilities.filters.default_fidelity.clone(),
            });
        } else {
            unsupported_predicates.push(predicate.clone());
        }
    }

    Ok(ScanPlan {
        plan_id: PlanId::new(format!("plan-{}", resource_id.as_str()))?,
        request,
        partitions,
        pushed_predicates,
        unsupported_predicates,
        estimated_rows,
        estimated_bytes,
        delivery_guarantee,
    })
}

pub fn datafusion_filter_pushdown(
    fidelity: &PushdownFidelity,
) -> datafusion::logical_expr::TableProviderFilterPushDown {
    match fidelity {
        PushdownFidelity::Exact => datafusion::logical_expr::TableProviderFilterPushDown::Exact,
        PushdownFidelity::Inexact => datafusion::logical_expr::TableProviderFilterPushDown::Inexact,
        PushdownFidelity::Unsupported => {
            datafusion::logical_expr::TableProviderFilterPushDown::Unsupported
        }
    }
}

pub(crate) fn validate_program(program: &ValidationProgram) -> Result<()> {
    assert_verdict_lattice_total(program)
}

fn validate_boundedness(boundedness: &PlanBoundedness) -> Result<()> {
    match boundedness {
        PlanBoundedness::Bounded | PlanBoundedness::UnboundedDrain => Ok(()),
        PlanBoundedness::UnboundedLive { .. } => Err(CdfError::contract(
            "unbounded live plans are illegal in the MVP; use drain mode or add cadence, rotation, and watermark support in a later ticket",
        )),
    }
}

fn residual_predicates(scan: &ScanPlan) -> Vec<ScanPredicate> {
    let mut residual = scan.unsupported_predicates.clone();
    residual.extend(
        scan.pushed_predicates
            .iter()
            .filter(|pushed| pushed.fidelity == PushdownFidelity::Inexact)
            .map(|pushed| pushed.predicate.clone()),
    );
    residual
}

fn operator_chain(
    resource_id: &ResourceId,
    projection: &Option<Vec<String>>,
    residual_predicates: &[ScanPredicate],
    limit: Option<u64>,
    program: &ValidationProgram,
    package_id: &str,
) -> Vec<OperatorNode> {
    vec![
        OperatorNode::CdfResourceAdapter {
            adapter_kind: CDF_NATIVE_RESOURCE_ADAPTER_KIND.to_owned(),
            resource_id: resource_id.clone(),
        },
        OperatorNode::CdfNativeScan {
            projection: projection.clone(),
            residual_predicates: residual_predicates
                .iter()
                .map(|predicate| predicate.expression.clone())
                .collect(),
            limit,
        },
        OperatorNode::SchemaFingerprintExec,
        OperatorNode::ContractExec {
            normalizer_version: program.normalizer_version.clone(),
            column_program_count: program.column_programs.len(),
        },
        OperatorNode::NormalizeExec {
            normalizer_version: program.normalizer_version.clone(),
        },
        OperatorNode::ProfileExec,
        OperatorNode::LineageExec,
        OperatorNode::PackageSink {
            package_id: package_id.to_owned(),
        },
    ]
}

fn explain_data(
    scan: &ScanPlan,
    boundedness: &PlanBoundedness,
    operator_chain: &[OperatorNode],
    projection_pushed: bool,
    limit_pushed: bool,
    estimate_support: EstimateSupport,
) -> ExplainData {
    let pushed_predicates = scan
        .pushed_predicates
        .iter()
        .map(|pushed| PredicateExplain {
            predicate_id: pushed.predicate.predicate_id.as_str().to_owned(),
            expression: pushed.predicate.expression.clone(),
            fidelity: pushed.fidelity.clone(),
        })
        .collect::<Vec<_>>();
    let inexact_predicates = pushed_predicates
        .iter()
        .filter(|predicate| predicate.fidelity == PushdownFidelity::Inexact)
        .cloned()
        .collect();
    let unsupported_predicates = scan
        .unsupported_predicates
        .iter()
        .map(|predicate| PredicateExplain {
            predicate_id: predicate.predicate_id.as_str().to_owned(),
            expression: predicate.expression.clone(),
            fidelity: PushdownFidelity::Unsupported,
        })
        .collect();

    ExplainData {
        resource_id: scan.request.resource_id.clone(),
        projected_fields: scan.request.projection.clone().unwrap_or_default(),
        projection_pushed,
        limit: scan.request.limit,
        limit_pushed,
        pushed_predicates,
        inexact_predicates,
        unsupported_predicates,
        partitions: scan
            .partitions
            .iter()
            .map(|partition| PartitionExplain {
                partition_id: partition.partition_id.as_str().to_owned(),
                scope_kind: format!("{:?}", partition.scope.kind()),
                metadata: partition.metadata.clone(),
            })
            .collect(),
        estimates: EstimateExplain {
            support: estimate_support,
            rows: scan.estimated_rows,
            bytes: scan.estimated_bytes,
        },
        delivery_guarantee: scan.delivery_guarantee.clone(),
        boundedness: boundedness.clone(),
        operator_chain: operator_chain.to_vec(),
    }
}

fn delivery_guarantee(disposition: WriteDisposition) -> DeliveryGuarantee {
    match disposition {
        WriteDisposition::Append => DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
        WriteDisposition::Merge => DeliveryGuarantee::EffectivelyOncePerKey,
        WriteDisposition::CdcApply => DeliveryGuarantee::EffectivelyOncePerPosition,
    }
}
