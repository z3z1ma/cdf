use std::collections::{BTreeMap, BTreeSet};

use cdf_contract::{
    ContractPolicy, ValidationProgram, assert_verdict_lattice_total,
    bind_validation_program_to_resource, reconcile_schema,
};
use cdf_kernel::{
    CapabilitySupport, CdfError, DeliveryGuarantee, EstimateSupport, PLAN_PHYSICAL_SCHEMA_HASH_KEY,
    PLAN_SCHEMA_OBSERVATION_BINDING_KEY, PLAN_SCHEMA_OBSERVATION_ID_KEY, PartitionPlan, PlanId,
    PushdownFidelity, QueryableResource, ResourceCapabilities, ResourceId, ResourceStream, Result,
    ScanPlan, ScanPredicate, ScanRequest, WriteDisposition,
};

use crate::{
    EffectiveSchemaObservationCoercion, EffectiveSchemaPlanEvidence, EngineOutputSchema,
    EnginePlan, EnginePlanInput, EngineSchemaAuthority, EstimateExplain, ExplainData, OperatorNode,
    PartitionExplain, PlanBoundedness, PredicateExplain, output_schema::compile_output_schema,
    predicates::predicate_operator,
};

pub const CDF_NATIVE_RESOURCE_ADAPTER_KIND: &str = "cdf_native_resource_adapter";

struct PlanFinishContext {
    write_disposition: WriteDisposition,
    projection_pushed: bool,
    limit_pushed: bool,
    estimate_support: EstimateSupport,
    output_schema: EngineOutputSchema,
    schema_authority: EngineSchemaAuthority,
}

#[derive(Debug, Default)]
pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self
    }

    pub fn plan_tier_a<R>(&self, resource: &R, mut input: EnginePlanInput) -> Result<EnginePlan>
    where
        R: ResourceStream + ?Sized,
    {
        input.validation_program =
            bind_validation_program_to_resource(input.validation_program, resource.descriptor())?;
        validate_boundedness(&input.boundedness)?;
        validate_program(&input.validation_program)?;
        let write_disposition = resource.descriptor().write_disposition.clone();

        let partitions = resource.plan_partitions(&input.request)?;
        let mut scan = ScanPlan {
            plan_id: PlanId::new(format!("plan-{}", input.request.resource_id.as_str()))?,
            request: input.request.clone(),
            partitions,
            pushed_predicates: Vec::new(),
            unsupported_predicates: input.request.filters.clone(),
            estimated_rows: None,
            estimated_bytes: None,
            delivery_guarantee: delivery_guarantee(write_disposition.clone()),
        };
        let effective_schema_evidence = bind_effective_schema_evidence(&mut scan, resource)?;
        let output_schema = EngineOutputSchema::from_arrow(
            compile_output_schema(
                resource.schema().as_ref(),
                &input.validation_program,
                input.request.projection.as_deref(),
                effective_schema_evidence.is_some(),
            )?
            .as_ref(),
        )?;
        let schema_authority = schema_authority(resource, effective_schema_evidence.as_ref())?;

        let mut plan = self.finish_plan(
            scan,
            input,
            PlanFinishContext {
                write_disposition,
                projection_pushed: false,
                limit_pushed: false,
                estimate_support: EstimateSupport::None,
                output_schema,
                schema_authority,
            },
        )?;
        plan.effective_schema_evidence = effective_schema_evidence;
        Ok(plan)
    }

    pub fn plan_tier_b<R>(&self, resource: &R, mut input: EnginePlanInput) -> Result<EnginePlan>
    where
        R: QueryableResource + ?Sized,
    {
        input.validation_program =
            bind_validation_program_to_resource(input.validation_program, resource.descriptor())?;
        validate_boundedness(&input.boundedness)?;
        validate_program(&input.validation_program)?;
        let write_disposition = resource.descriptor().write_disposition.clone();

        let mut scan = resource.negotiate(&input.request)?;
        let effective_schema_evidence = bind_effective_schema_evidence(&mut scan, resource)?;
        let output_schema = EngineOutputSchema::from_arrow(
            compile_output_schema(
                resource.schema().as_ref(),
                &input.validation_program,
                input.request.projection.as_deref(),
                effective_schema_evidence.is_some(),
            )?
            .as_ref(),
        )?;
        let schema_authority = schema_authority(resource, effective_schema_evidence.as_ref())?;
        let mut plan = self.finish_plan(
            scan,
            input,
            PlanFinishContext {
                write_disposition,
                projection_pushed: resource.capabilities().projection
                    == CapabilitySupport::Supported,
                limit_pushed: resource.capabilities().limits == CapabilitySupport::Supported,
                estimate_support: resource.capabilities().estimates.clone(),
                output_schema,
                schema_authority,
            },
        )?;
        plan.effective_schema_evidence = effective_schema_evidence;
        Ok(plan)
    }

    fn finish_plan(
        &self,
        scan: ScanPlan,
        input: EnginePlanInput,
        finish: PlanFinishContext,
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
            finish.projection_pushed,
            finish.limit_pushed,
            finish.estimate_support,
        );

        Ok(EnginePlan {
            scan,
            partition_schedule: None,
            operator_graph: None,
            effective_schema_evidence: None,
            final_projection,
            residual_predicates,
            boundedness: input.boundedness,
            write_disposition: finish.write_disposition,
            validation_program: input.validation_program,
            schema_authority: Some(finish.schema_authority),
            output_schema: Some(finish.output_schema),
            operator_chain,
            explain,
            package_id: input.package_id,
        })
    }
}

fn bind_effective_schema_evidence<R>(
    scan: &mut ScanPlan,
    resource: &R,
) -> Result<Option<EffectiveSchemaPlanEvidence>>
where
    R: ResourceStream + ?Sized,
{
    let Some(runtime) = resource.effective_schema_runtime() else {
        return Ok(None);
    };
    runtime.validate_for_resource(resource.descriptor())?;
    let evidence = &runtime.evidence;
    let mut observation_bindings = BTreeMap::new();
    for partition in &mut scan.partitions {
        let observation_id = partition
            .metadata
            .get(PLAN_SCHEMA_OBSERVATION_ID_KEY)
            .ok_or_else(|| {
            CdfError::data(
                "effective schema evidence requires every planned partition to identify its schema observation",
            )
        })?;
        let observation = evidence.observation(observation_id).ok_or_else(|| {
            CdfError::data(format!(
                "effective schema evidence has no candidate for planned observation {observation_id:?}"
            ))
        })?;
        let binding = partition
            .metadata
            .get(PLAN_SCHEMA_OBSERVATION_BINDING_KEY)
            .ok_or_else(|| {
                CdfError::data(format!(
                    "effective schema observation {observation_id:?} omitted its source identity binding"
                ))
            })?
            .clone();
        if observation_bindings
            .insert(observation_id.clone(), binding.clone())
            .is_some_and(|existing| existing != binding)
        {
            return Err(CdfError::data(format!(
                "repeated effective schema observation {observation_id:?} carries conflicting source identity bindings"
            )));
        }
        partition.metadata.insert(
            PLAN_PHYSICAL_SCHEMA_HASH_KEY.to_owned(),
            observation.physical_schema_hash.to_string(),
        );
    }
    let mut type_policy =
        ContractPolicy::for_trust(resource.descriptor().trust_level.clone()).types;
    type_policy.coerce_types = false;
    type_policy.allow_lossy_mapping = false;
    for physical in &runtime.schema_catalog {
        let computed_hash = cdf_contract::canonical_arrow_schema_hash(physical.schema.as_ref())?;
        if computed_hash != physical.physical_schema_hash {
            return Err(CdfError::data(format!(
                "physical schema catalog entry {} does not match its canonical schema hash {}",
                physical.physical_schema_hash, computed_hash
            )));
        }
    }
    let observations = evidence
        .observations
        .iter()
        .filter(|observation| {
            runtime
                .terminal_quarantine(&observation.observation_id)
                .is_none()
        })
        .map(|observation| {
            let physical_schema = runtime
                .physical_schema(&observation.physical_schema_hash)
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "effective schema runtime omitted physical schema {} for observation {:?}",
                        observation.physical_schema_hash, observation.observation_id
                    ))
                })?;
            let reconciliation = reconcile_schema(
                physical_schema.as_ref(),
                resource.schema().as_ref(),
                &type_policy,
            )?;
            validate_reconciliation_target(&reconciliation.schema, resource.schema().as_ref())?;
            Ok(EffectiveSchemaObservationCoercion {
                observation_id: observation.observation_id.clone(),
                physical_schema_hash: observation.physical_schema_hash.clone(),
                coercion_plan: reconciliation.plan,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(EffectiveSchemaPlanEvidence {
        authority: evidence.clone(),
        effective_arrow_schema_hash: cdf_contract::canonical_arrow_schema_hash(
            resource.schema().as_ref(),
        )?,
        observations,
        terminal_quarantines: runtime.terminal_quarantines.clone(),
        discovery_executor_budget: runtime.discovery_executor_budget.clone(),
        observation_bindings,
    }))
}

pub(crate) fn schema_authority<R>(
    resource: &R,
    effective: Option<&EffectiveSchemaPlanEvidence>,
) -> Result<EngineSchemaAuthority>
where
    R: ResourceStream + ?Sized,
{
    if let Some(effective) = effective {
        return Ok(EngineSchemaAuthority {
            version: 1,
            baseline_schema_hash: effective.authority.baseline_snapshot.schema_hash.clone(),
            effective_schema_hash: effective.authority.effective_snapshot_schema_hash.clone(),
        });
    }
    let schema_hash = match &resource.descriptor().schema_source {
        cdf_kernel::SchemaSource::Declared { schema_hash, .. } => schema_hash.clone(),
        cdf_kernel::SchemaSource::Discovered { snapshot } => snapshot.schema_hash.clone(),
        cdf_kernel::SchemaSource::Hints {
            snapshot: Some(snapshot),
            ..
        } => snapshot.schema_hash.clone(),
        cdf_kernel::SchemaSource::Contract {
            schema_hash: Some(schema_hash),
            ..
        } => schema_hash.clone(),
        _ => cdf_contract::canonical_arrow_schema_hash(resource.schema().as_ref())?,
    };
    Ok(EngineSchemaAuthority {
        version: 1,
        baseline_schema_hash: schema_hash.clone(),
        effective_schema_hash: schema_hash,
    })
}

pub fn validate_plan_schema_authority<R>(resource: &R, plan: &EnginePlan) -> Result<()>
where
    R: ResourceStream + ?Sized,
{
    let expected_authority = schema_authority(resource, plan.effective_schema_evidence.as_ref())?;
    if plan.schema_authority.as_ref() != Some(&expected_authority) {
        return Err(CdfError::data(
            "engine plan schema authority does not match the execution resource",
        ));
    }
    let expected_output = EngineOutputSchema::from_arrow(
        compile_output_schema(
            resource.schema().as_ref(),
            &plan.validation_program,
            plan.final_projection.as_deref(),
            plan.effective_schema_evidence.is_some(),
        )?
        .as_ref(),
    )?;
    if plan.output_schema.as_ref() != Some(&expected_output) {
        return Err(CdfError::data(
            "engine plan compiled output schema does not match the resource, projection, and validation program",
        ));
    }
    Ok(())
}

fn validate_reconciliation_target(
    reconciled: &arrow_schema::Schema,
    effective: &arrow_schema::Schema,
) -> Result<()> {
    if reconciled.fields().len() != effective.fields().len()
        || reconciled
            .fields()
            .iter()
            .zip(effective.fields())
            .any(|(left, right)| {
                left.name() != right.name()
                    || left.data_type() != right.data_type()
                    || left.is_nullable() != right.is_nullable()
                    || cdf_kernel::source_name(left.as_ref()).unwrap_or_else(|| left.name())
                        != cdf_kernel::source_name(right.as_ref()).unwrap_or_else(|| right.name())
            })
    {
        return Err(CdfError::data(
            "schema reconciliation did not target the exact effective Arrow schema",
        ));
    }
    Ok(())
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
            segmentation: crate::CanonicalSegmentationPolicy::p3_v1(),
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
        partition_schedule: None,
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
