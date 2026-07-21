use std::collections::{BTreeMap, BTreeSet};

use cdf_contract::{
    CompiledExpressionPlan, ContractPolicy, ExpressionUse, TransformDescription, ValidationProgram,
    assert_verdict_lattice_total, bind_validation_program_to_resource, reconcile_schema,
};
use cdf_kernel::{
    CapabilitySupport, CdfError, CompiledScanIntent, DeliveryGuarantee, EstimateSupport,
    ExecutionExtent, PLAN_PHYSICAL_SCHEMA_HASH_KEY, PLAN_SCHEMA_OBSERVATION_BINDING_KEY,
    PLAN_SCHEMA_OBSERVATION_ID_KEY, PartitionAuthority, PartitionPlan, PlanId, PushdownFidelity,
    QueryableResource, ResourceCapabilities, ResourceId, ResourceStream, Result, ScanPlan,
    ScanPredicate, ScanRequest, WriteDisposition,
};

use crate::{
    CompiledArrowSchema, CompiledSchemaAdmissionPlan, EffectiveSchemaObservationCoercion,
    EffectiveSchemaPlanEvidence, EnginePlan, EnginePlanInput, EngineSchemaAuthority,
    EstimateExplain, ExplainData, OperatorNode, PartitionExplain, PredicateExplain,
    expression::{
        mark_cursor_subsumed, plan_expression, record_exact_source_expression,
        record_native_contract_expression, validate_recorded_expressions,
    },
    output_schema::compile_output_schema,
};

pub const CDF_NATIVE_RESOURCE_ADAPTER_KIND: &str = "cdf_native_resource_adapter";

struct PlanFinishContext {
    write_disposition: WriteDisposition,
    projection_pushed: bool,
    limit_pushed: bool,
    estimate_support: EstimateSupport,
    output_schema: CompiledArrowSchema,
    schema_authority: EngineSchemaAuthority,
    resource_schema: arrow_schema::Schema,
    schema_admission_constraint: arrow_schema::Schema,
    type_policy: cdf_contract::TypePolicy,
    expression_schema: arrow_schema::Schema,
    cursor_field: Option<String>,
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
        validate_execution_extent(&input.execution_extent)?;
        validate_program(&input.validation_program)?;
        let write_disposition = resource.descriptor().write_disposition.clone();

        let partitions = resource.plan_partitions(&input.request)?;
        validate_tier_a_partition_intents(&partitions)?;
        let mut scan = ScanPlan::from_partition_authority(
            PlanId::new(format!("plan-{}", input.request.resource_id.as_str()))?,
            input.request.clone(),
            PartitionAuthority::Inline(partitions),
            Vec::new(),
            input.request.filters.clone(),
            None,
            None,
            delivery_guarantee(write_disposition.clone()),
        );
        cdf_kernel::validate_scan_partition_observation_identities(&scan)?;
        let effective_schema_evidence = bind_effective_schema_evidence(&mut scan, resource)?;
        let output_schema = CompiledArrowSchema::from_arrow(
            compile_output_schema(
                resource.schema().as_ref(),
                &input.validation_program,
                input.request.projection.as_deref(),
                effective_schema_evidence.is_some(),
            )?
            .as_ref(),
        )?;
        let schema_authority = schema_authority(resource, effective_schema_evidence.as_ref())?;
        let resource_schema = resource.schema().as_ref().clone();
        let type_policy = resource_type_policy(resource);

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
                resource_schema: resource_schema.clone(),
                schema_admission_constraint: resource_schema.clone(),
                type_policy,
                expression_schema: resource_schema,
                cursor_field: resource
                    .descriptor()
                    .cursor
                    .as_ref()
                    .map(|cursor| cursor.field.clone()),
            },
        )?;
        plan.compiled_schema_admission
            .bind_baseline_schema_catalog(
                resource.baseline_observation_schema_catalog(),
                resource.schema().as_ref(),
                None,
            )?;
        if let Some(evidence) = &effective_schema_evidence {
            plan.compiled_schema_admission
                .validate_preobserved_evidence(evidence)?;
        }
        plan.effective_schema_evidence = effective_schema_evidence;
        Ok(plan)
    }

    pub fn plan_tier_b<R>(&self, resource: &R, mut input: EnginePlanInput) -> Result<EnginePlan>
    where
        R: QueryableResource + ?Sized,
    {
        input.validation_program =
            bind_validation_program_to_resource(input.validation_program, resource.descriptor())?;
        validate_execution_extent(&input.execution_extent)?;
        validate_program(&input.validation_program)?;
        let write_disposition = resource.descriptor().write_disposition.clone();

        let mut required_fields = resource.descriptor().primary_key.clone();
        required_fields.extend(resource.descriptor().merge_key.iter().cloned());
        if let Some(cursor) = resource.descriptor().cursor.as_ref() {
            required_fields.push(cursor.field.clone());
        }
        let physical_request = physical_scan_request(
            &input.request,
            resource.schema().as_ref(),
            &input.validation_program,
            &required_fields,
        )?;
        let mut scan = resource.negotiate(&physical_request)?;
        validate_negotiated_scan(&physical_request, &scan, resource.capabilities())?;
        cdf_kernel::validate_scan_partition_observation_identities(&scan)?;
        let effective_schema_evidence = bind_effective_schema_evidence(&mut scan, resource)?;
        let output_schema = CompiledArrowSchema::from_arrow(
            compile_output_schema(
                resource.schema().as_ref(),
                &input.validation_program,
                input.request.projection.as_deref(),
                effective_schema_evidence.is_some(),
            )?
            .as_ref(),
        )?;
        let schema_authority = schema_authority(resource, effective_schema_evidence.as_ref())?;
        let resource_schema = resource.schema().as_ref().clone();
        let schema_admission_constraint = scan_expression_schema(
            &resource_schema,
            (resource.capabilities().projection == CapabilitySupport::Supported)
                .then_some(scan.request.projection.as_deref())
                .flatten(),
        )?;
        let type_policy = resource_type_policy(resource);
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
                resource_schema: resource_schema.clone(),
                schema_admission_constraint,
                type_policy,
                expression_schema: resource_schema,
                cursor_field: resource
                    .descriptor()
                    .cursor
                    .as_ref()
                    .map(|cursor| cursor.field.clone()),
            },
        )?;
        let baseline_projection = (resource.capabilities().projection
            == CapabilitySupport::Supported)
            .then_some(plan.scan.request.projection.as_deref())
            .flatten();
        plan.compiled_schema_admission
            .bind_baseline_schema_catalog(
                resource.baseline_observation_schema_catalog(),
                resource.schema().as_ref(),
                baseline_projection,
            )?;
        if let Some(evidence) = &effective_schema_evidence {
            plan.compiled_schema_admission
                .validate_preobserved_evidence(evidence)?;
        }
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
        let mut predicate_expressions = scan
            .request
            .filters
            .iter()
            .map(|predicate| {
                let exact_source_pushdown = scan.pushed_predicates.iter().any(|pushed| {
                    pushed.predicate.predicate_id == predicate.predicate_id
                        && pushed.fidelity == PushdownFidelity::Exact
                });
                let mut planned = if exact_source_pushdown {
                    record_exact_source_expression(predicate.canonical_expression.clone())?
                } else {
                    plan_expression(
                        predicate.canonical_expression.clone(),
                        ExpressionUse::Filter,
                        &finish.expression_schema,
                    )?
                };
                planned.source_text = Some(predicate.expression.clone());
                Ok(planned)
            })
            .collect::<Result<Vec<_>>>()?;
        if let Some(cursor_field) = finish.cursor_field.as_deref() {
            mark_cursor_subsumed(&mut predicate_expressions, cursor_field);
        }
        let residual_expressions = residual_predicates
            .iter()
            .map(|predicate| {
                let mut planned = plan_expression(
                    predicate.canonical_expression.clone(),
                    ExpressionUse::Filter,
                    &finish.expression_schema,
                )?;
                planned.source_text = Some(predicate.expression.clone());
                Ok(planned)
            })
            .collect::<Result<Vec<_>>>()?;
        validate_recorded_expressions(&predicate_expressions)?;
        validate_recorded_expressions(&residual_expressions)?;
        let (transform_expressions, contract_schema) =
            plan_transform_expressions(&input.validation_program, &finish.expression_schema)?;
        let contract_expressions = input
            .validation_program
            .row_rules
            .iter()
            .map(|rule| {
                record_native_contract_expression(rule.expression.clone(), &contract_schema)
            })
            .collect::<Result<Vec<_>>>()?;
        validate_recorded_expressions(&contract_expressions)?;
        cdf_contract::bind_vector_validation_plan(
            &input.validation_program,
            std::sync::Arc::new(contract_schema),
        )?;
        let compiled_expression_plan = CompiledExpressionPlan::current(
            predicate_expressions,
            residual_expressions,
            contract_expressions,
            transform_expressions,
        )?;
        compiled_expression_plan.validate_recorded()?;
        let mut validation_program = input.validation_program;
        validation_program.compiled_expression_plan = Some(compiled_expression_plan.clone());
        let compiled_schema_admission = CompiledSchemaAdmissionPlan::compile(
            &finish.schema_authority,
            &finish.resource_schema,
            &finish.schema_admission_constraint,
            &validation_program,
            finish.type_policy,
        )?;
        let final_projection = input.request.projection.clone();
        let operator_chain = operator_chain(
            &scan.request.resource_id,
            &final_projection,
            &residual_predicates,
            scan.request.limit,
            &validation_program,
            &input.package_id,
        );
        let explain = explain_data(
            &scan,
            &input.execution_extent,
            &operator_chain,
            finish.projection_pushed,
            finish.limit_pushed,
            finish.estimate_support,
        );

        Ok(EnginePlan {
            scan,
            compiled_source_execution: None,
            partition_schedule: None,
            operator_graph: None,
            compiled_stream_policy: None,
            effective_schema_evidence: None,
            final_projection,
            residual_predicates,
            compiled_expression_plan,
            compiled_schema_admission,
            execution_extent: input.execution_extent,
            write_disposition: finish.write_disposition,
            validation_program,
            schema_authority: finish.schema_authority,
            output_schema: finish.output_schema,
            operator_chain,
            explain,
            package_id: input.package_id,
        })
    }
}

fn validate_tier_a_partition_intents(partitions: &[PartitionPlan]) -> Result<()> {
    let full_scan = CompiledScanIntent::full_scan();
    for partition in partitions {
        if partition.scan_intent != full_scan {
            return Err(CdfError::contract(format!(
                "Tier-A partition `{}` compiled source pushdown; ResourceStream partitions must use a full-scan intent so engine projection, filtering, ordering, and limits remain authoritative",
                partition.partition_id
            )));
        }
    }
    Ok(())
}

fn physical_scan_request(
    request: &ScanRequest,
    schema: &arrow_schema::Schema,
    program: &ValidationProgram,
    required_fields: &[String],
) -> Result<ScanRequest> {
    let Some(requested) = request
        .projection
        .as_ref()
        .filter(|fields| !fields.is_empty())
    else {
        return Ok(request.clone());
    };
    let mut dependencies = requested.iter().cloned().collect::<BTreeSet<_>>();
    dependencies.extend(
        request
            .filters
            .iter()
            .flat_map(|predicate| predicate.canonical_expression.column_dependencies()),
    );
    for transform in &program.transforms {
        match transform {
            TransformDescription::Rename { from, .. } => {
                dependencies.insert(from.clone());
            }
            TransformDescription::Cast { column, .. }
            | TransformDescription::Redact { column, .. }
            | TransformDescription::ExpandNested { column, .. } => {
                dependencies.insert(column.clone());
            }
            TransformDescription::Derive { expression, .. }
            | TransformDescription::Filter { expression } => {
                dependencies.extend(expression.column_dependencies());
            }
        }
    }
    dependencies.extend(
        program
            .row_rules
            .iter()
            .flat_map(|rule| rule.expression.column_dependencies()),
    );
    dependencies.extend(required_fields.iter().cloned());

    let projection = schema
        .fields()
        .iter()
        .filter(|field| dependencies.contains(field.name()))
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    for dependency in dependencies {
        if schema.index_of(&dependency).is_err()
            && !program.transforms.iter().any(|transform| {
                matches!(transform, TransformDescription::Derive { column, .. } if column == &dependency)
                    || matches!(transform, TransformDescription::Rename { to, .. } if to == &dependency)
            })
        {
            return Err(CdfError::contract(format!(
                "scan projection dependency {dependency:?} is absent from the source schema and is not produced by a transform"
            )));
        }
    }
    let mut physical = request.clone();
    physical.projection = Some(projection);
    Ok(physical)
}

pub(crate) fn scan_expression_schema(
    schema: &arrow_schema::Schema,
    projection: Option<&[String]>,
) -> Result<arrow_schema::Schema> {
    let Some(projection) = projection.filter(|fields| !fields.is_empty()) else {
        return Ok(schema.clone());
    };
    let fields = projection
        .iter()
        .map(|name| {
            schema
                .field_with_name(name)
                .cloned()
                .map(std::sync::Arc::new)
                .map_err(|_| {
                    CdfError::contract(format!(
                        "physical scan projection field {name:?} is absent from the resource schema"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(arrow_schema::Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}

fn validate_negotiated_scan(
    request: &ScanRequest,
    scan: &ScanPlan,
    capabilities: &ResourceCapabilities,
) -> Result<()> {
    if &scan.request != request {
        return Err(CdfError::contract(
            "source negotiation changed the canonical scan request",
        ));
    }
    let mut classified = BTreeMap::new();
    for predicate in scan
        .pushed_predicates
        .iter()
        .map(|pushed| &pushed.predicate)
        .chain(scan.unsupported_predicates.iter())
    {
        if classified
            .insert(predicate.predicate_id.as_str(), predicate)
            .is_some()
        {
            return Err(CdfError::contract(format!(
                "source negotiation classified predicate {} more than once",
                predicate.predicate_id
            )));
        }
    }
    if request.filters.len() != classified.len()
        || request.filters.iter().any(|predicate| {
            classified.get(predicate.predicate_id.as_str()).copied() != Some(predicate)
        })
    {
        return Err(CdfError::contract(
            "source negotiation did not classify each canonical predicate exactly once",
        ));
    }
    let supported_operators = capabilities
        .filters
        .supported_operators
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    for pushed in &scan.pushed_predicates {
        let operator = pushed
            .predicate
            .canonical_expression
            .comparison_operator()
            .map(str::to_owned);
        if capabilities.filters.default_fidelity == PushdownFidelity::Unsupported
            || pushed.fidelity == PushdownFidelity::Unsupported
            || operator
                .as_deref()
                .is_none_or(|operator| !supported_operators.contains(operator))
        {
            return Err(CdfError::contract(format!(
                "source pushed predicate {} outside its declared filter capabilities",
                pushed.predicate.predicate_id
            )));
        }
    }
    cdf_kernel::validate_compiled_scan_intents(scan)?;
    for partition in scan.inline_partitions().unwrap_or(&[]) {
        let expected_projection = (capabilities.projection == CapabilitySupport::Supported)
            .then(|| request.projection.clone())
            .flatten();
        if partition.scan_intent.projection != expected_projection {
            return Err(CdfError::contract(format!(
                "source projection capability does not match compiled intent for partition {}",
                partition.partition_id
            )));
        }
        let expected_limit = if capabilities.limits == CapabilitySupport::Supported {
            request.limit
        } else {
            None
        };
        if partition.scan_intent.limit != expected_limit {
            return Err(CdfError::contract(format!(
                "source limit capability does not match compiled intent for partition {}",
                partition.partition_id
            )));
        }
        let expected_order = if capabilities.ordering == CapabilitySupport::Supported {
            request.order_by.as_slice()
        } else {
            &[]
        };
        if partition.scan_intent.order_by.as_slice() != expected_order {
            return Err(CdfError::contract(format!(
                "source ordering capability does not match compiled intent for partition {}",
                partition.partition_id
            )));
        }
    }
    Ok(())
}

fn plan_transform_expressions(
    program: &ValidationProgram,
    schema: &arrow_schema::Schema,
) -> Result<(Vec<cdf_contract::PlannedExpression>, arrow_schema::Schema)> {
    let mut expression_schema = schema.clone();
    let mut planned = Vec::new();
    for transform in &program.transforms {
        let (use_kind, source_text, expression) = match transform {
            TransformDescription::Derive { column, expression } => (
                ExpressionUse::Derive,
                Some(column.clone()),
                expression.clone(),
            ),
            TransformDescription::Filter { expression } => {
                (ExpressionUse::Filter, None, expression.clone())
            }
            _ => continue,
        };
        let mut expression = plan_expression(expression, use_kind, &expression_schema)?;
        expression.source_text = source_text.clone();
        planned.push(expression);

        if let Some(column) = source_text {
            let field = std::sync::Arc::new(arrow_schema::Field::new(
                column,
                arrow_schema::DataType::Boolean,
                true,
            ));
            let mut fields = expression_schema
                .fields()
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            if let Ok(index) = expression_schema.index_of(field.name()) {
                fields[index] = field;
            } else {
                fields.push(field);
            }
            expression_schema = arrow_schema::Schema::new_with_metadata(
                fields,
                expression_schema.metadata().clone(),
            );
        }
    }
    Ok((planned, expression_schema))
}

pub(crate) fn rebind_validation_program(
    plan: &mut EnginePlan,
    mut program: ValidationProgram,
    expression_schema: &arrow_schema::Schema,
) -> Result<()> {
    validate_program(&program)?;
    let mut candidate = plan.clone();
    let physical_expression_schema = if candidate.explain.projection_pushed {
        let mut final_request = candidate.scan.request.clone();
        final_request
            .projection
            .clone_from(&candidate.final_projection);
        let existing_projection = candidate
            .scan
            .request
            .projection
            .clone()
            .unwrap_or_default();
        let required = physical_scan_request(
            &final_request,
            expression_schema,
            &program,
            &existing_projection,
        )?;
        if required.projection != candidate.scan.request.projection {
            return Err(CdfError::contract(
                "replacement validation program requires source fields outside the compiled physical projection; replan the resource",
            ));
        }
        scan_expression_schema(expression_schema, required.projection.as_deref())?
    } else {
        expression_schema.clone()
    };
    let (transforms, contract_schema) =
        plan_transform_expressions(&program, &physical_expression_schema)?;
    cdf_contract::bind_vector_validation_plan(
        &program,
        std::sync::Arc::new(contract_schema.clone()),
    )?;
    let contracts = program
        .row_rules
        .iter()
        .map(|rule| record_native_contract_expression(rule.expression.clone(), &contract_schema))
        .collect::<Result<Vec<_>>>()?;
    validate_recorded_expressions(&contracts)?;
    let compiled_expression_plan = cdf_contract::CompiledExpressionPlan::current(
        candidate.compiled_expression_plan.predicates.clone(),
        candidate.compiled_expression_plan.residuals.clone(),
        contracts,
        transforms,
    )?;
    compiled_expression_plan.validate_recorded()?;
    program.compiled_expression_plan = Some(compiled_expression_plan.clone());
    let source_binding = candidate.compiled_schema_admission.source.clone();
    let baseline_projection = candidate
        .compiled_schema_admission
        .baseline_projection
        .clone();
    let baseline_projected_schema_hashes = candidate
        .compiled_schema_admission
        .baseline_projected_schema_hashes
        .clone();
    let mut compiled_schema_admission = CompiledSchemaAdmissionPlan::compile(
        &candidate.schema_authority,
        expression_schema,
        &physical_expression_schema,
        &program,
        candidate.compiled_schema_admission.type_policy.clone(),
    )?;
    compiled_schema_admission.baseline_projection = baseline_projection;
    compiled_schema_admission.baseline_projected_schema_hashes = baseline_projected_schema_hashes;
    compiled_schema_admission.source = source_binding;
    let output_schema = CompiledArrowSchema::from_arrow(
        compile_output_schema(
            expression_schema,
            &program,
            candidate.final_projection.as_deref(),
            candidate.effective_schema_evidence.is_some(),
        )?
        .as_ref(),
    )?;
    candidate.compiled_expression_plan = compiled_expression_plan;
    candidate.compiled_schema_admission = compiled_schema_admission;
    candidate.validation_program = program;
    candidate.output_schema = output_schema;
    candidate.operator_chain = operator_chain(
        &candidate.scan.request.resource_id,
        &candidate.final_projection,
        &candidate.residual_predicates,
        candidate.scan.request.limit,
        &candidate.validation_program,
        &candidate.package_id,
    );
    candidate
        .explain
        .operator_chain
        .clone_from(&candidate.operator_chain);
    candidate.validate_compiled_expression_plan()?;
    candidate
        .compiled_schema_admission
        .validate_intrinsic(&candidate.validation_program)?;
    *plan = candidate;
    Ok(())
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
    let projection = match scan.partition_authority() {
        cdf_kernel::PartitionAuthority::Inline(partitions) => partitions
            .first()
            .and_then(|partition| partition.scan_intent.projection.clone()),
        cdf_kernel::PartitionAuthority::External(_) => scan.request.projection.clone(),
    };
    let admission_constraint =
        scan_expression_schema(resource.schema().as_ref(), projection.as_deref())?;
    let projected_observations = evidence
        .observations
        .iter()
        .filter(|observation| {
            runtime
                .terminal_quarantine(&observation.observation_id)
                .is_none()
        })
        .map(|observation| {
            let physical = runtime
                .physical_schema(&observation.physical_schema_hash)
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "effective schema runtime omitted physical schema {} for observation {:?}",
                        observation.physical_schema_hash, observation.observation_id
                    ))
                })?;
            let projected = project_physical_observation(
                physical.as_ref(),
                resource.schema().as_ref(),
                projection.as_deref(),
            )?;
            let hash = cdf_kernel::canonical_arrow_schema_hash(&projected)?;
            Ok((observation.observation_id.clone(), (hash, projected)))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut observation_bindings = evidence
        .observations
        .iter()
        .map(|observation| {
            (
                observation.observation_id.clone(),
                observation.schema_observation_binding.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut assigned_observations = BTreeSet::new();
    let inline_partitions = match scan.partition_authority() {
        cdf_kernel::PartitionAuthority::Inline(_) => Some(
            scan.inline_partitions_mut()
                .expect("inline partition authority was matched"),
        ),
        cdf_kernel::PartitionAuthority::External(_) => {
            // External task records retain the source-authored observation binding. The
            // registry-validated planned-partition reader checks each record as it streams; the
            // engine must not enumerate or rewrite the external authority during planning.
            None
        }
    };
    if let Some(inline_partitions) = inline_partitions {
        for partition in inline_partitions {
            let observation_id = partition
            .metadata
            .get(PLAN_SCHEMA_OBSERVATION_ID_KEY)
            .ok_or_else(|| {
            CdfError::data(
                "effective schema evidence requires every planned partition to identify its schema observation",
            )
        })?;
            let binding = cdf_kernel::SchemaObservationBinding::new(
                partition
                .metadata
                .get(PLAN_SCHEMA_OBSERVATION_BINDING_KEY)
                .ok_or_else(|| {
                CdfError::data(format!(
                    "effective schema observation {observation_id:?} omitted its source identity binding"
                ))
            })?
            .clone(),
            )?;
            if !assigned_observations.insert(observation_id.clone()) {
                return Err(CdfError::data(format!(
                    "effective schema observation {observation_id:?} is assigned to more than one planned partition; observation identities must be partition-scoped"
                )));
            }
            match evidence.observation(observation_id) {
                Some(observation) => {
                    if observation.schema_observation_binding != binding {
                        return Err(CdfError::data(format!(
                            "effective schema observation {observation_id:?} does not match its planned partition source identity"
                        )));
                    }
                    let execution_hash = projected_observations
                        .get(observation_id)
                        .map(|(hash, _)| hash)
                        .unwrap_or(&observation.physical_schema_hash);
                    partition.metadata.insert(
                        PLAN_PHYSICAL_SCHEMA_HASH_KEY.to_owned(),
                        execution_hash.to_string(),
                    );
                }
                None => {
                    observation_bindings.insert(observation_id.clone(), binding);
                    partition.metadata.remove(PLAN_PHYSICAL_SCHEMA_HASH_KEY);
                }
            }
        }
    }
    let mut type_policy =
        ContractPolicy::for_trust(resource.descriptor().trust_level.clone()).types;
    let allowances = resource.type_policy_allowances();
    type_policy.coerce_types = allowances.coerce_types;
    type_policy.allow_lossy_mapping = allowances.allow_lossy_mapping;
    for physical in &runtime.schema_catalog {
        let computed_hash = cdf_kernel::canonical_arrow_schema_hash(physical.schema.as_ref())?;
        if computed_hash != physical.physical_schema_hash {
            return Err(CdfError::data(format!(
                "physical schema catalog entry {} does not match its canonical schema hash {}",
                physical.physical_schema_hash, computed_hash
            )));
        }
    }
    let physical_observation_catalog = evidence
        .observations
        .iter()
        .map(|observation| {
            let physical_schema = if let Some((_, projected)) =
                projected_observations.get(&observation.observation_id)
            {
                projected
            } else {
                runtime
                    .physical_schema(&observation.physical_schema_hash)
                    .map(AsRef::as_ref)
                    .ok_or_else(|| {
                        CdfError::data(format!(
                            "effective schema runtime omitted physical schema {} for observation {:?}",
                            observation.physical_schema_hash, observation.observation_id
                        ))
                    })?
            };
            Ok((
                cdf_kernel::canonical_arrow_schema_hash(physical_schema)?.to_string(),
                crate::PhysicalObservationEvidence::arrow_schema(physical_schema)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let observations = evidence
        .observations
        .iter()
        .filter(|observation| {
            runtime
                .terminal_quarantine(&observation.observation_id)
                .is_none()
        })
        .map(|observation| {
            let (physical_schema_hash, physical_schema) = projected_observations
                .get(&observation.observation_id)
                .ok_or_else(|| {
                    CdfError::internal(format!(
                        "admitted effective schema observation {:?} omitted its execution projection",
                        observation.observation_id
                    ))
                })?;
            let reconciliation = reconcile_schema(
                physical_schema,
                &admission_constraint,
                &type_policy,
            )?;
            validate_reconciliation_target(&reconciliation.schema, &admission_constraint)?;
            Ok(EffectiveSchemaObservationCoercion {
                observation_id: observation.observation_id.clone(),
                physical_schema_hash: physical_schema_hash.clone(),
                coercion_plan: reconciliation.plan,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(EffectiveSchemaPlanEvidence {
        authority: evidence.clone(),
        effective_arrow_schema_hash: cdf_kernel::canonical_arrow_schema_hash(
            resource.schema().as_ref(),
        )?,
        observations,
        physical_observation_catalog,
        terminal_quarantines: runtime.terminal_quarantines.clone(),
        discovery_executor_budget: runtime.discovery_executor_budget.clone(),
        observation_bindings,
    }))
}

fn project_physical_observation(
    physical: &arrow_schema::Schema,
    effective: &arrow_schema::Schema,
    projection: Option<&[String]>,
) -> Result<arrow_schema::Schema> {
    let Some(projection) = projection.filter(|fields| !fields.is_empty()) else {
        return Ok(physical.clone());
    };
    let fields = projection
        .iter()
        .map(|logical_name| {
            let effective_field = effective.field_with_name(logical_name).map_err(|_| {
                CdfError::contract(format!(
                    "compiled scan projection field {logical_name:?} is absent from the effective schema"
                ))
            })?;
            let physical_name = cdf_kernel::source_name(effective_field)
                .unwrap_or_else(|| effective_field.name());
            physical
                .fields()
                .iter()
                .find(|field| {
                    field.name() == physical_name
                        || cdf_kernel::source_name(field.as_ref()) == Some(physical_name)
                })
                .cloned()
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "physical schema observation omitted projected source field {physical_name:?} for effective field {logical_name:?}"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(arrow_schema::Schema::new_with_metadata(
        fields,
        physical.metadata().clone(),
    ))
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
            baseline_schema_hash: effective.authority.baseline.schema_hash().clone(),
            effective_schema_hash: effective.authority.effective_schema_hash.clone(),
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
        _ => cdf_kernel::canonical_arrow_schema_hash(resource.schema().as_ref())?,
    };
    Ok(EngineSchemaAuthority {
        version: 1,
        baseline_schema_hash: schema_hash.clone(),
        effective_schema_hash: schema_hash,
    })
}

fn resource_type_policy<R>(resource: &R) -> cdf_contract::TypePolicy
where
    R: ResourceStream + ?Sized,
{
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone()).types;
    let allowances = resource.type_policy_allowances();
    policy.coerce_types = allowances.coerce_types;
    policy.allow_lossy_mapping = allowances.allow_lossy_mapping;
    policy
}

pub fn validate_plan_schema_authority<R>(resource: &R, plan: &EnginePlan) -> Result<()>
where
    R: ResourceStream + ?Sized,
{
    let expected_authority = schema_authority(resource, plan.effective_schema_evidence.as_ref())?;
    if plan.schema_authority != expected_authority {
        return Err(CdfError::data(
            "engine plan schema authority does not match the execution resource",
        ));
    }
    let expected_output = CompiledArrowSchema::from_arrow(
        compile_output_schema(
            resource.schema().as_ref(),
            &plan.validation_program,
            plan.final_projection.as_deref(),
            plan.effective_schema_evidence.is_some(),
        )?
        .as_ref(),
    )?;
    if plan.output_schema != expected_output {
        return Err(CdfError::data(
            "engine plan compiled output schema does not match the resource, projection, and validation program",
        ));
    }
    plan.validate_compiled_schema_admission(resource)?;
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
    mut partitions: Vec<PartitionPlan>,
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
        let operator = predicate
            .canonical_expression
            .comparison_operator()
            .map(str::to_owned);
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

    let intent = CompiledScanIntent {
        version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
        projection: if capabilities.projection == CapabilitySupport::Supported {
            request.projection.clone()
        } else {
            None
        },
        predicates: pushed_predicates.clone(),
        limit: if capabilities.limits == CapabilitySupport::Supported {
            request.limit
        } else {
            None
        },
        order_by: if capabilities.ordering == CapabilitySupport::Supported {
            request.order_by.clone()
        } else {
            Vec::new()
        },
    };
    intent.validate()?;
    for partition in &mut partitions {
        partition.scan_intent = intent.clone();
    }

    Ok(ScanPlan::from_partition_authority(
        PlanId::new(format!("plan-{}", resource_id.as_str()))?,
        request,
        PartitionAuthority::Inline(partitions),
        pushed_predicates,
        unsupported_predicates,
        estimated_rows,
        estimated_bytes,
        delivery_guarantee,
    ))
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

fn validate_execution_extent(execution_extent: &ExecutionExtent) -> Result<()> {
    execution_extent.validate_for_plan()
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
            segmentation: crate::CanonicalSegmentationPolicy::p3_v2(),
        },
    ]
}

fn explain_data(
    scan: &ScanPlan,
    execution_extent: &ExecutionExtent,
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
            .inline_partitions()
            .unwrap_or(&[])
            .iter()
            .map(|partition| PartitionExplain {
                partition_id: partition.partition_id.as_str().to_owned(),
                scope_kind: format!("{:?}", partition.scope.kind()),
                metadata: partition.metadata.clone(),
            })
            .collect(),
        partition_schedule: None,
        compiled_stream_policy: None,
        estimates: EstimateExplain {
            support: estimate_support,
            rows: scan.estimated_rows,
            bytes: scan.planned_source_bytes.map(|bytes| bytes.get()),
        },
        delivery_guarantee: scan.delivery_guarantee.clone(),
        execution_extent: execution_extent.clone(),
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

#[cfg(test)]
mod expression_transform_tests {
    use std::sync::Arc;

    use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use cdf_contract::{
        CompiledExpressionPlan, ContractPolicy, Expression, ObservedSchema, RowRule,
        TransformDescription, compile_validation_program,
    };
    use cdf_kernel::{PredicateId, ResourceId, ScanPredicate, ScanRequest, ScopeKey};

    use super::{
        physical_scan_request, plan_transform_expressions, record_native_contract_expression,
        scan_expression_schema,
    };
    use crate::expression::plan_expression;
    use cdf_expression::{
        apply_bound_filters, apply_expression_transforms, bind_filter_expressions,
    };

    #[test]
    fn physical_projection_closes_over_expression_and_contract_dependencies_in_source_order() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("other", DataType::Int64, false),
            Field::new("updated_at", DataType::Int64, false),
        ]);
        let mut policy = ContractPolicy::evolve();
        policy.transforms = vec![TransformDescription::Derive {
            column: "selected".to_owned(),
            expression: Expression::parse_comparison("id >= 2").unwrap(),
        }];
        policy.rows.rules = vec![RowRule::Nullability {
            column: "selected".to_owned(),
        }];
        let program =
            compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
        let request = ScanRequest {
            resource_id: ResourceId::new("orders").unwrap(),
            projection: Some(vec!["other".to_owned(), "selected".to_owned()]),
            filters: vec![
                ScanPredicate::new(PredicateId::new("residual-id").unwrap(), "id != 1").unwrap(),
            ],
            limit: None,
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        };

        let physical =
            physical_scan_request(&request, &schema, &program, &["updated_at".to_owned()]).unwrap();
        assert_eq!(
            physical.projection,
            Some(vec![
                "id".to_owned(),
                "other".to_owned(),
                "updated_at".to_owned(),
            ])
        );
        let physical_schema =
            scan_expression_schema(&schema, physical.projection.as_deref()).unwrap();
        assert_eq!(physical_schema.field(0).name(), "id");
        assert_eq!(physical_schema.field(1).name(), "other");

        let planned = plan_expression(
            request.filters[0].canonical_expression.clone(),
            cdf_contract::ExpressionUse::Filter,
            &physical_schema,
        )
        .unwrap();
        let bound = bind_filter_expressions(&[planned], &physical_schema).unwrap();
        let dishonest_projected_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "other",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let error = apply_bound_filters(&dishonest_projected_batch, &bound).unwrap_err();
        assert!(error.to_string().contains("changed to \"other\""));
    }

    #[test]
    fn derive_then_filter_share_one_sequential_compiled_expression_plan() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
        let mut policy = ContractPolicy::evolve();
        policy.transforms = vec![
            TransformDescription::Derive {
                column: "selected".to_owned(),
                expression: Expression::parse_comparison("id >= 2").unwrap(),
            },
            TransformDescription::Filter {
                expression: Expression::parse_comparison("selected = true").unwrap(),
            },
        ];
        policy.rows.rules = vec![RowRule::Nullability {
            column: "selected".to_owned(),
        }];
        let mut program =
            compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
        let (planned, contract_schema) = plan_transform_expressions(&program, &schema).unwrap();
        cdf_contract::bind_vector_validation_plan(&program, Arc::new(contract_schema.clone()))
            .unwrap();
        let projected = crate::output_schema::compile_output_schema(
            &schema,
            &program,
            Some(&["selected".to_owned()]),
            false,
        )
        .unwrap();
        assert_eq!(projected.field(0).name(), "selected");
        assert!(
            projected
                .fields()
                .iter()
                .any(|field| field.name() == cdf_contract::VARIANT_COLUMN_NAME)
        );
        let contracts = program
            .row_rules
            .iter()
            .map(|rule| {
                record_native_contract_expression(rule.expression.clone(), &contract_schema)
                    .unwrap()
            })
            .collect();
        let compiled =
            CompiledExpressionPlan::current(Vec::new(), Vec::new(), contracts, planned.clone())
                .unwrap();
        program.compiled_expression_plan = Some(compiled.clone());
        compiled.validate_program_binding(&program).unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]))],
        )
        .unwrap();
        let transformed =
            apply_expression_transforms(batch, &program.transforms, &planned).unwrap();
        assert_eq!(transformed.num_rows(), 1);
        assert_eq!(
            transformed
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            3
        );
        assert!(
            transformed
                .column_by_name("selected")
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
    }
}
