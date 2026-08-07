mod render;

use std::{collections::BTreeMap, fs};

use cdf_contract::{
    ContractPolicy, IdentifierPolicy, ObservedSchema, compile_resource_validation_program,
};
use cdf_declarative::CompiledResource;
use cdf_engine::{
    CanonicalSegmentationPolicy, EnginePlan, EnginePlanInput, EnginePreviewLimits,
    EnginePreviewSelectionEvidence, Planner,
};
use cdf_kernel::{
    CapabilitySupport, CdfError, CheckpointStore, DeliveryGuarantee, DestinationSheet,
    IdempotencySupport, OrderBy, PartitionPlan, PipelineId, PredicateId, QueryableResource,
    ResourceStream, ScanPredicate, ScanRequest, SchemaSource, SortDirection,
    SourceDiscoveryBinding, SourcePosition, TargetName, TransactionSupport, WriteDisposition,
};
use serde::Serialize;

use crate::{
    args::{Cli, PlanArgs, ScanArgs},
    commands::json_cli_error,
    context::ProjectContext,
    destination_uri::{
        EnvironmentDestination, redact_error_value, resolve_selected_destination,
        resolve_selected_destination_with_services,
    },
    error_catalog,
    output::{CliError, CommandOutput},
    project_run_resource::{
        CliProjectRunSource, compile_source_plan_for_cli,
        discover_source_schema_with_plan_for_cli_at,
        prepare_runtime_resource_for_cli_with_artifact_root,
    },
    reports::{DiscoveryCoverageReport, SchemaSnapshotActionReport, WriteEffects},
    run_command::DEFAULT_RUN_PIPELINE_ID,
};

pub(crate) struct PreparedSchemaForCli {
    pub(crate) resource: CompiledResource,
    pub(crate) source_plan: cdf_runtime::CompiledSourcePlan,
    pub(crate) schema_snapshot: Option<SchemaSnapshotActionReport>,
    pub(crate) prepared_payloads: cdf_runtime::PreparedSourcePayloads,
    pub(crate) schema_artifact_files: Vec<(String, Vec<u8>)>,
}

impl PreparedSchemaForCli {
    fn new(
        resource: CompiledResource,
        schema_snapshot: Option<SchemaSnapshotActionReport>,
        prepared_payloads: cdf_runtime::PreparedSourcePayloads,
        schema_artifact_files: Vec<(String, Vec<u8>)>,
    ) -> Result<Self, CliError> {
        let source_plan = resource.source_plan().clone();
        validate_resource_source_authority(&source_plan)?;
        let source_schema = resource
            .relational_expression_plan()
            .map(|plan| plan.input_schema.to_arrow())
            .transpose()?
            .unwrap_or_else(|| resource.schema().as_ref().clone());
        let mut expected_source_descriptor = resource.descriptor().clone();
        expected_source_descriptor.schema_source = source_plan.descriptor.schema_source.clone();
        if expected_source_descriptor != source_plan.descriptor {
            return Err(CdfError::contract(
                "compiled source plan changed non-schema resource authority",
            )
            .into());
        }
        source_plan.validate_schema_authority(
            &source_plan.descriptor,
            &source_schema,
            resource.effective_schema_runtime(),
            resource.baseline_observation_schema_catalog(),
        )?;
        Ok(Self {
            resource,
            source_plan,
            schema_snapshot,
            prepared_payloads,
            schema_artifact_files,
        })
    }
}

pub(crate) fn validate_resource_source_authority(
    source_plan: &cdf_runtime::CompiledSourcePlan,
) -> cdf_kernel::Result<()> {
    if let Some(snapshot) = source_plan.descriptor.schema_source.cached_snapshot() {
        validate_recorded_source_authority(
            &snapshot.metadata,
            source_plan.driver.driver_id.as_str(),
            &source_plan.driver.driver_version,
            &source_plan.discovery_binding_hash()?,
        )?;
    }
    Ok(())
}

fn validate_recorded_source_authority(
    metadata: &BTreeMap<String, String>,
    driver_id: &str,
    driver_version: &str,
    discovery_binding: &SourceDiscoveryBinding,
) -> cdf_kernel::Result<()> {
    let recorded_driver = metadata
        .get("source_driver")
        .ok_or_else(|| CdfError::data("schema snapshot omitted its source driver"))?;
    let recorded_version = metadata.get("source_driver_version").ok_or_else(|| {
        CdfError::data("registered-source schema snapshot omitted its source driver version")
    })?;
    let recorded_discovery_binding = metadata
        .get("source_discovery_binding")
        .ok_or_else(|| {
            CdfError::data("registered-source schema snapshot omitted its source discovery binding")
        })?
        .parse::<SourceDiscoveryBinding>()?;
    if recorded_driver != driver_id
        || recorded_version != driver_version
        || &recorded_discovery_binding != discovery_binding
    {
        return Err(CdfError::data(format!(
            "active schema observation provenance `{recorded_driver}`/{recorded_version}/{recorded_discovery_binding} does not match source authority `{driver_id}`/{driver_version}/{discovery_binding}; run `cdf schema diff` for the selected resource, then promote or recompile as appropriate",
        )));
    }
    Ok(())
}

pub(crate) fn plan(
    cli: &Cli,
    args: PlanArgs,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let (root, project_file) = crate::context::project_location(cli.project.as_ref())?;
    let project_text = fs::read_to_string(&project_file).map_err(|error| {
        crate::context::project_authority_read_error(
            "read project configuration",
            &project_file,
            error,
        )
    })?;
    let config = cdf_project::parse_cdf_toml(&project_text)?;
    let environment = config.effective_environment(
        cli.env
            .as_deref()
            .unwrap_or(&config.project.default_environment),
    )?;
    crate::schema_authority::reset_proposal_seed();
    if let Some(path) = args.out.as_deref()
        && let Ok(existing) = crate::portable_plan_command::load_artifact(path)
        && existing.project_id == config.project.id
        && existing.environment == environment.name
    {
        crate::schema_authority::seed_portable_proposals(&existing);
    }
    let selection =
        cdf_project::resolve_project_resource_selection(&root, &args.selectors, &args.exclude)
            .map_err(|error| crate::compile_command::resource_selection_error("cdf plan", error))?;
    let mut resources = Vec::with_capacity(selection.resources.len());
    let mut portable_resources = Vec::with_capacity(selection.resources.len());
    for selected in &selection.resources {
        let resource_id = selected.resource_id.to_string();
        match plan_one(
            cli,
            &args,
            &resource_id,
            execution,
            destinations,
            args.out.is_some(),
        ) {
            Ok(planned) => {
                if let Some(portable) = planned.portable {
                    portable_resources.push(portable);
                }
                resources.push(PlanResourceOutcome::Ready {
                    report: Box::new(planned.report),
                });
            }
            Err(error) => resources.push(PlanResourceOutcome::Failed {
                resource_id,
                error: PlanResourceError::from(error),
            }),
        }
    }
    let ready = resources
        .iter()
        .filter(|result| matches!(result, PlanResourceOutcome::Ready { .. }))
        .count();
    let failed = resources.len() - ready;
    let artifact = if failed == 0 {
        args.out
            .as_deref()
            .map(|path| {
                crate::portable_plan_command::build_artifact(
                    &root,
                    &config,
                    &environment,
                    selection.selection.clone(),
                    portable_resources,
                )
                .and_then(|artifact| {
                    crate::portable_plan_command::publish_artifact(path, &artifact)
                })
            })
            .transpose()?
    } else {
        None
    };
    let report = PlanReport {
        project: config.project.name,
        environment: environment.name,
        selection: selection.selection,
        counts: PlanCounts {
            selected: resources.len(),
            ready,
            failed,
        },
        resources,
        artifact,
    };
    CommandOutput::rendered_with_exit_code(
        "plan",
        render::plan_report_document(&report),
        &report,
        i32::from(failed != 0),
    )
}

fn plan_one(
    cli: &Cli,
    args: &PlanArgs,
    resource_id: &str,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
    portable: bool,
) -> Result<PlannedResource, CliError> {
    let scan = ScanArgs {
        resource_id: resource_id.to_owned(),
        destination_uri: args.destination_uri.clone(),
        projection: args.projection.clone(),
        filters: args.filters.clone(),
        limit: args.limit,
        order_by: args.order_by.clone(),
        segmentation: args.segmentation.clone(),
    };
    scan_one_with_portable(cli, &scan, "plan", execution, destinations, portable)
}

struct PlannedResource {
    report: ScanPlanReport,
    portable: Option<crate::portable_plan_command::PortablePlanResourceMaterial>,
}

pub(crate) fn plan_or_explain(
    cli: &Cli,
    args: ScanArgs,
    command: &'static str,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let report = scan_one(cli, &args, command, execution, destinations)?;
    CommandOutput::rendered(command, render::scan_report_document(&report), report)
}

pub(crate) fn scan_one(
    cli: &Cli,
    args: &ScanArgs,
    command: &'static str,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<ScanPlanReport, CliError> {
    scan_one_with_portable(cli, args, command, execution, destinations, false)
        .map(|planned| planned.report)
}

fn scan_one_with_portable(
    cli: &Cli,
    args: &ScanArgs,
    command: &'static str,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
    portable: bool,
) -> Result<PlannedResource, CliError> {
    let context = ProjectContext::load_selected_read_only(
        cli.project.as_ref(),
        cli.env.as_deref(),
        &args.resource_id,
        destinations,
    )?;
    let inspection_root = tempfile::Builder::new()
        .prefix("cdf-plan-")
        .tempdir()
        .map_err(|error| {
            CdfError::environment(format!(
                "create inspection artifact root in the host temporary directory: {error}; check temporary-directory access, free space, and process file limits before retrying"
            ))
        })?;
    let target = scan_target(&context, args)?;
    let prepared = prepare_runtime_resource_for_cli_with_artifact_root(
        &context,
        &args.resource_id,
        false,
        Some(execution),
        inspection_root.path(),
    )?;
    let schema_authority = crate::schema_authority::prepare(&context, &prepared.compiled_resource)?;
    let resolved = resolve_scan_destination(
        destinations,
        &context,
        &target,
        args.destination_uri.as_deref(),
        command,
        portable.then_some(execution),
    )?;
    let committed_frontier = planning_frontier(
        &context,
        prepared.resource.as_queryable().descriptor(),
        &PipelineId::new(DEFAULT_RUN_PIPELINE_ID)?,
    )?;
    let plan = build_engine_plan_for_resource(
        &prepared.resource,
        args,
        None,
        committed_frontier,
        &resolved.destination.runtime_capabilities(),
    )?;
    let portable = portable
        .then(|| {
            let destination_uri = args
                .destination_uri
                .as_deref()
                .unwrap_or(&context.environment.destination);
            crate::portable_plan_command::build_resource_material(
                &context,
                &prepared,
                &schema_authority,
                plan.clone(),
                &resolved,
                destination_uri,
                inspection_root.path(),
            )
        })
        .transpose()?;
    let report = scan_report(
        &context,
        &prepared.resource,
        &plan,
        ScanReportPresentation {
            command,
            destination_uri: args
                .destination_uri
                .as_deref()
                .map(crate::render::redaction::redact_uri_userinfo),
            schema_snapshot: prepared.schema_snapshot,
            schema_authority: SchemaAuthorityReport::from_prepared(&schema_authority)?,
        },
        resolved,
        execution,
    )?;
    Ok(PlannedResource { report, portable })
}

pub(crate) fn preview(
    cli: &Cli,
    args: ScanArgs,
    host: &cdf_engine::StandaloneExecutionHost,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load_selected_read_only(
        cli.project.as_ref(),
        cli.env.as_deref(),
        &args.resource_id,
        destinations,
    )?;
    let inspection_root = tempfile::Builder::new()
        .prefix("cdf-preview-")
        .tempdir()
        .map_err(|error| {
            CdfError::environment(format!(
                "create preview artifact root in the host temporary directory: {error}; check temporary-directory access, free space, and process file limits before retrying"
            ))
        })?;
    let prepared = prepare_runtime_resource_for_cli_with_artifact_root(
        &context,
        &args.resource_id,
        false,
        Some(execution),
        inspection_root.path(),
    )?;
    let target = scan_target(&context, &args)?;
    let resolved = resolve_scan_destination(
        destinations,
        &context,
        &target,
        args.destination_uri.as_deref(),
        "preview",
        None,
    )?;
    let plan = build_engine_plan_for_resource(
        &prepared.resource,
        &args,
        None,
        None,
        &resolved.destination.runtime_capabilities(),
    )?;
    match preview_resource_report(&prepared.resource, &plan, prepared.schema_snapshot, host) {
        Ok(report) => CommandOutput::rendered("preview", render::preview_document(&report), report),
        Err(error) if lower_runtime_missing(&error) => Err(CliError::not_supported_with(
            "preview",
            error.message,
            "resource runtime open implementation",
            error_catalog::PREVIEW_RUNTIME_NOT_SUPPORTED,
        )),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn prepare_resource_schema_for_cli(
    context: &ProjectContext,
    resource: &CompiledResource,
    commit_schema: bool,
    execution: Option<&cdf_runtime::ExecutionServices>,
    artifact_root: &std::path::Path,
) -> Result<PreparedSchemaForCli, CliError> {
    let prepared_payloads = cdf_runtime::PreparedSourcePayloads::default();
    let active = crate::schema_authority::load_active(context, &resource.descriptor().resource_id)?;
    if let Some(active) = active.as_ref()
        && let Some(compiled) = active_compiled_source_resource(context, active)?
    {
        let prepared = bind_active_logical_schema(context, compiled, Some(active))?;
        return PreparedSchemaForCli::new(prepared, None, prepared_payloads, Vec::new());
    }
    let source_plan = compile_source_plan_for_cli(resource)?;
    if let Some(snapshot) = resource.descriptor().schema_source.cached_snapshot() {
        let prepared =
            cdf_project::prepare_cached_resource_schema_artifacts(&context.root, resource)?;
        let discovery = prepared
            .discovery_manifest()
            .map(DiscoveryCoverageReport::from_manifest);
        let (prepared, _) = prepared.into_parts();
        let prepared = finalize_resource_query_for_cli(context, prepared)?;
        let prepared = bind_active_logical_schema(context, prepared, active.as_ref())?;
        return PreparedSchemaForCli::new(
            prepared,
            Some(SchemaSnapshotActionReport {
                outcome: "unchanged",
                schema_hash: snapshot.schema_hash.to_string(),
                path: snapshot.path.clone(),
                snapshot_written: false,
                discovery,
            }),
            prepared_payloads,
            Vec::new(),
        );
    }
    let probe_resource = resource.clone();
    if !matches!(
        probe_resource.descriptor().schema_source,
        SchemaSource::Discover | SchemaSource::Hints { snapshot: None, .. }
    ) {
        let prepared = finalize_resource_query_for_cli(context, resource.clone())?;
        let prepared = bind_active_logical_schema(context, prepared, active.as_ref())?;
        return PreparedSchemaForCli::new(prepared, None, prepared_payloads, Vec::new());
    }
    let options = match resource.descriptor().schema_source.cached_snapshot() {
        Some(snapshot) => {
            let (_, verified_baseline) = cdf_project::SchemaSnapshotStore::new(&context.root)
                .read_with_verified_baseline(snapshot)?;
            cdf_project::SchemaDiscoveryExecutionOptions::new()
                .with_verified_baseline(verified_baseline)
        }
        None => cdf_project::SchemaDiscoveryExecutionOptions::new(),
    }
    .with_observation_cache(cdf_project::ObservationCacheStore::new(artifact_root));
    let execution = execution
        .ok_or_else(|| CdfError::internal("source discovery requires execution services"))?;
    let discovery_plan = source_plan.clone().bind_schema_authority(
        probe_resource.descriptor(),
        probe_resource.schema().as_ref(),
        probe_resource.effective_schema_runtime().cloned(),
        probe_resource
            .baseline_observation_schema_catalog()
            .to_vec(),
    )?;
    let mut artifacts = discover_source_schema_with_plan_for_cli_at(
        context,
        &probe_resource,
        &discovery_plan,
        execution,
        prepared_payloads.clone(),
        options,
        artifact_root,
    )?;
    let prepared_resource =
        cdf_project::compile_discovered_schema_artifacts(&probe_resource, &mut artifacts)?;
    let prepared_resource = finalize_resource_query_for_cli(context, prepared_resource)?;
    let prepared_resource =
        bind_active_logical_schema(context, prepared_resource, active.as_ref())?;
    let discovery_coverage = artifacts
        .discovery_manifest
        .as_ref()
        .map(DiscoveryCoverageReport::from_manifest);
    let artifact = artifacts.discovery.snapshot.artifact.clone();
    let schema_artifact_files = artifacts.canonical_artifact_files()?;
    let outcome = if commit_schema {
        "added"
    } else {
        "inspection_only"
    };
    let snapshot_written = if !commit_schema {
        false
    } else {
        cdf_project::write_schema_discovery_artifacts(&context.root, &artifacts)?.snapshot_written
    };
    PreparedSchemaForCli::new(
        prepared_resource,
        Some(SchemaSnapshotActionReport {
            outcome,
            schema_hash: artifact.schema_hash.to_string(),
            path: artifact.path.clone(),
            snapshot_written,
            discovery: discovery_coverage,
        }),
        prepared_payloads,
        schema_artifact_files,
    )
}

fn active_compiled_source_resource(
    context: &ProjectContext,
    active: &crate::schema_authority::ActiveSchemaAuthority,
) -> Result<Option<CompiledResource>, CliError> {
    let snapshot = cdf_project::load_compilation_snapshot(
        &context.root,
        Some(context.environment.name.as_str()),
    )?;
    let resource_id = active.head.key.resource_id.as_str();
    let Some(artifact) = snapshot.artifacts.get(resource_id) else {
        return Ok(None);
    };
    if artifact.schema_authority.key != active.head.key
        || artifact.schema_authority.generation != active.head.generation
        || artifact.schema_authority.schema_hash != active.head.schema_hash
    {
        return Ok(None);
    }
    Ok(Some(cdf_project::hydrate_compiled_resource_artifact(
        &context.root,
        artifact,
    )?))
}

fn bind_active_logical_schema(
    context: &ProjectContext,
    prepared: CompiledResource,
    active: Option<&crate::schema_authority::ActiveSchemaAuthority>,
) -> Result<CompiledResource, CliError> {
    let Some(active) = active else {
        return Ok(prepared);
    };
    let logical =
        cdf_project::compiled_logical_output_schema(&prepared, &context.semantic_catalog)?;
    let logical_hash = cdf_kernel::canonical_arrow_schema_hash(&logical.to_arrow()?)?;
    if logical_hash != active.head.schema_hash {
        return Err(CdfError::contract(format!(
            "resource `{}` compiles to schema {} but active state authority is generation {} schema {}; run `cdf schema promote {}` to review the logical schema change",
            prepared.descriptor().resource_id,
            logical_hash,
            active.head.generation,
            active.head.schema_hash,
            prepared.descriptor().resource_id,
        ))
        .into());
    }
    Ok(prepared.with_logical_schema_source(SchemaSource::Active {
        schema_hash: active.head.schema_hash.clone(),
    })?)
}

fn finalize_resource_query_for_cli(
    context: &ProjectContext,
    resource: CompiledResource,
) -> Result<CompiledResource, CliError> {
    if resource.relational_expression_plan().is_some() || resource.schema().fields().is_empty() {
        return Ok(resource);
    }
    let resource_id = resource.descriptor().resource_id.as_str();
    let query = match context.resource_query(resource_id).cloned() {
        Some(query) => query,
        None if context.is_adhoc_resource(resource_id) => return Ok(resource),
        None => {
            return Err(CdfError::internal(format!(
                "compiled resource {resource_id:?} lost its project query authority"
            ))
            .into());
        }
    };
    Ok(cdf_project::finalize_query_project_resource(
        cdf_project::CompiledProjectResource { resource, query },
        &context.semantic_catalog,
    )?
    .resource)
}

pub(crate) fn build_engine_plan_for_resource(
    source: &crate::project_run_resource::CliProjectRunSource,
    args: &ScanArgs,
    run_package_id: Option<&str>,
    committed_frontier: Option<SourcePosition>,
    destination_capabilities: &cdf_runtime::DestinationRuntimeCapabilities,
) -> Result<EnginePlan, CliError> {
    let resource = source.as_queryable();
    let logical_schema = source
        .relational_expression_plan()
        .map(|plan| plan.output_schema.to_arrow())
        .transpose()?
        .unwrap_or_else(|| resource.schema().as_ref().clone());
    let observed_schema = ObservedSchema::from_arrow(&logical_schema);
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    let allowances = resource.type_policy_allowances();
    policy.types.coerce_types = allowances.coerce_types;
    policy.types.allow_lossy_mapping = allowances.allow_lossy_mapping;
    let validation_program =
        compile_resource_validation_program(&policy, &observed_schema, resource.descriptor())?;
    let request = scan_request(resource.descriptor(), args)?;
    let input = EnginePlanInput {
        request,
        validation_program,
        execution_extent: source.execution_extent().clone(),
        segmentation: segmentation_policy_from_tuning(&args.segmentation)?,
        package_id: run_package_id
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("cli-{}", resource.descriptor().resource_id)),
        relational_expression_plan: source.relational_expression_plan().cloned(),
        committed_frontier,
    };
    let plan = Planner::new()
        .plan_tier_b(resource, input)
        .map_err(CliError::from)?;
    let source_plan = source.source_plan();
    plan.bind_compiled_source(source_plan)
        .and_then(|plan| plan.bind_operator_graph(source_plan, destination_capabilities))
        .map_err(CliError::from)
}

pub(crate) fn planning_frontier(
    context: &ProjectContext,
    descriptor: &cdf_kernel::ResourceDescriptor,
    pipeline_id: &PipelineId,
) -> Result<Option<SourcePosition>, CliError> {
    let state_path = context.state_store_path()?;
    let ownership = context.state_store_path_ownership();
    planning_frontier_at(&state_path, ownership, descriptor, pipeline_id)
}

pub(crate) fn planning_frontier_at(
    state_path: &std::path::Path,
    ownership: cdf_state_sqlite::StateStorePathOwnership,
    descriptor: &cdf_kernel::ResourceDescriptor,
    pipeline_id: &PipelineId,
) -> Result<Option<SourcePosition>, CliError> {
    if !cdf_state_sqlite::SqliteCheckpointStore::is_initialized(state_path, ownership)? {
        return Ok(None);
    }
    let frontier = cdf_state_sqlite::SqliteCheckpointStore::open_read_only_with_path_ownership(
        state_path, ownership,
    )?
    .head(
        pipeline_id,
        &descriptor.resource_id,
        &descriptor.state_scope,
    )?
    .map(|checkpoint| checkpoint.delta.source_resume_position().clone())
    // File resources already compute a changed/unchanged summary while binding their
    // manifest in project orchestration. Until that summary is compiled into ScanPlan,
    // preserve its single existing binding point instead of filtering the task set twice.
    .filter(|position| !matches!(position, SourcePosition::FileManifest(_)));
    Ok(frontier)
}

pub(crate) fn segmentation_policy_from_tuning(
    tuning: &cdf_cli_core::args::SegmentationArgs,
) -> Result<CanonicalSegmentationPolicy, CliError> {
    let mut policy = CanonicalSegmentationPolicy::performance_default();
    policy.target_rows = tuning.target_rows.unwrap_or(policy.target_rows);
    policy.target_bytes = tuning.target_bytes.unwrap_or(policy.target_bytes);
    policy.maximum_rows = tuning.maximum_rows.unwrap_or(policy.maximum_rows);
    policy.maximum_bytes = tuning.maximum_bytes.unwrap_or(policy.maximum_bytes);
    policy.microbatch_minimum_rows = tuning
        .microbatch_minimum_rows
        .unwrap_or(policy.microbatch_minimum_rows);
    policy.microbatch_maximum_rows = tuning
        .microbatch_maximum_rows
        .unwrap_or(policy.microbatch_maximum_rows);
    policy.microbatch_minimum_bytes = tuning
        .microbatch_minimum_bytes
        .unwrap_or(policy.microbatch_minimum_bytes);
    policy.microbatch_maximum_bytes = tuning
        .microbatch_maximum_bytes
        .unwrap_or(policy.microbatch_maximum_bytes);
    policy.validate().map_err(CliError::from)?;
    Ok(policy)
}

fn scan_request(
    descriptor: &cdf_kernel::ResourceDescriptor,
    args: &ScanArgs,
) -> Result<ScanRequest, CliError> {
    let filters = args
        .filters
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            ScanPredicate::new(
                PredicateId::new(format!("p{:03}", index + 1))?,
                expression.clone(),
            )
        })
        .collect::<cdf_kernel::Result<Vec<_>>>()?;
    Ok(ScanRequest {
        resource_id: descriptor.resource_id.clone(),
        projection: args.projection.clone(),
        filters,
        limit: args.limit,
        order_by: args
            .order_by
            .iter()
            .map(|order| parse_order_by(order))
            .collect::<Result<Vec<_>, _>>()?,
        scope: descriptor.state_scope.clone(),
    })
}

fn parse_order_by(raw: &str) -> Result<OrderBy, CliError> {
    let (field, direction) = raw.split_once(':').unwrap_or((raw, "asc"));
    let direction = match direction {
        "asc" => SortDirection::Asc,
        "desc" => SortDirection::Desc,
        other => {
            return Err(CliError::usage_with(
                format!("unsupported order direction `{other}`"),
                error_catalog::SCAN_ARGUMENT,
            ));
        }
    };
    Ok(OrderBy {
        field: field.to_owned(),
        direction,
    })
}

fn scan_report(
    context: &ProjectContext,
    resource: &CliProjectRunSource,
    plan: &EnginePlan,
    presentation: ScanReportPresentation,
    resolved: EnvironmentDestination,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<ScanPlanReport, CliError> {
    let source = resource.source_plan();
    let partition_count = plan.scan.partition_count()?;
    let scheduler = Some(cdf_runtime::resolve_runtime_scheduler(
        partition_count,
        &source.execution_capabilities,
        &resolved.destination.runtime_capabilities(),
        execution,
        None,
    )?);
    let queryable = resource.as_queryable();
    let destination_plan =
        destination_plan_report(resolved, queryable, plan, presentation.command)?;
    let output_schema = plan.output_arrow_schema()?;
    Ok(ScanPlanReport {
        human_command: presentation.command,
        human_destination_uri: presentation.destination_uri,
        project: context.config.project.name.clone(),
        environment: context.environment.name.clone(),
        resource_id: plan.scan.request.resource_id.to_string(),
        resource_schema: resource_schema_report(
            queryable,
            &destination_plan.schema_hash,
            output_schema.as_ref(),
            plan.effective_schema_evidence(),
        ),
        normalization: plan.validation_program.identifier_policy.clone(),
        admission: AdmissionPlanReport {
            dispositions: plan.validation_program.admission.clone(),
            observation_strength: if presentation.schema_authority.status == "active" {
                "runtime_stream"
            } else {
                "bounded_first_use_discovery"
            },
            wider_source_observation: plan.validation_program.admission.field
                == cdf_contract::FieldDisposition::CaptureVariant
                && plan.final_projection.is_some()
                && plan.scan.request.projection.is_none(),
            source_schema_migrations: 0,
        },
        will_fetch: FetchReport {
            partition_count: plan.scan.partition_count()?,
            partitions: plan
                .scan
                .inline_partitions()
                .unwrap_or_default()
                .iter()
                .map(partition_report)
                .collect(),
            projection: plan.final_projection.clone().unwrap_or_default(),
            filters: (if plan.residual_predicates.is_empty() {
                &plan.scan.request.filters
            } else {
                &plan.residual_predicates
            })
            .iter()
            .map(|predicate| predicate.expression.clone())
            .collect(),
            limit: plan.final_limit.or(plan.scan.request.limit),
        },
        pushdown: PushdownReport {
            pushed: plan.explain.pushed_predicates.clone(),
            inexact: plan.explain.inexact_predicates.clone(),
            unsupported: plan.explain.unsupported_predicates.clone(),
        },
        destination: destination_plan.destination,
        ddl_preview: destination_plan.ddl_preview,
        delivery_guarantee: destination_plan.delivery_guarantee.guarantee.clone(),
        delivery_guarantee_detail: destination_plan.delivery_guarantee,
        state_advancement: StateAdvancementReport {
            scope: serde_json::to_value(&queryable.descriptor().state_scope)
                .map_err(json_cli_error)?,
            cursor: queryable
                .descriptor()
                .cursor
                .as_ref()
                .map(|cursor| cursor.field.clone()),
            advances_after:
                "destination receipt is recorded and CheckpointStore::commit verifies coverage"
                    .to_owned(),
        },
        explain: plan.explain.clone(),
        operator_graph: plan.operator_graph.clone(),
        scheduler,
        package_id: plan.package_id.clone(),
        schema_snapshot: presentation.schema_snapshot,
        schema_authority: presentation.schema_authority,
    })
}

fn scan_target(context: &ProjectContext, args: &ScanArgs) -> Result<TargetName, CliError> {
    context.resource_target(&args.resource_id).cloned()
}

fn destination_plan_report(
    resolved: EnvironmentDestination,
    resource: &dyn QueryableResource,
    engine_plan: &EnginePlan,
    command: &'static str,
) -> Result<DestinationPlanReport, CliError> {
    let mut destination = resolved.destination;
    let plan = destination
        .plan_resource_commit(resource, engine_plan)
        .map_err(|error| {
            let mut error = redact_error_value(error, resolved.secret_redaction.as_deref());
            error.message = command_correct_scan_message(command, error.message);
            CliError::from(error)
        })?;
    DestinationPlanReport::from_project(plan, resource).map_err(CliError::from)
}

fn resolve_scan_destination(
    destinations: &cdf_runtime::DestinationRegistry,
    context: &ProjectContext,
    target: &TargetName,
    destination_uri: Option<&str>,
    command: &'static str,
    services: Option<&cdf_runtime::ExecutionServices>,
) -> Result<EnvironmentDestination, CliError> {
    let resolved = match services {
        Some(services) => resolve_selected_destination_with_services(
            destinations,
            context,
            target,
            destination_uri,
            Some(services),
        ),
        None => resolve_selected_destination(destinations, context, target, destination_uri),
    };
    resolved.map_err(|error| {
        plan_destination_resolution_error(command, context, destination_uri, error)
    })
}

fn command_correct_scan_message(command: &str, message: String) -> String {
    if command == "run" {
        message
    } else {
        message.replace("cdf run ", &format!("cdf {command} "))
    }
}

fn plan_destination_resolution_error(
    command: &'static str,
    context: &ProjectContext,
    destination_uri: Option<&str>,
    error: CdfError,
) -> CliError {
    let error = redact_error_value(error, None);
    if error
        .message
        .contains("no project destination driver registered")
        || error.message.contains("malformed or non-local")
        || error.message.contains("is missing a scheme")
    {
        CliError::not_supported_with(
            command,
            error.message,
            "registered no-write project destination planner",
            error_catalog::DESTINATION_NOT_SUPPORTED,
        )
        .with_suggestions(crate::destination_uri::destination_error_suggestions(
            context,
            destination_uri,
        ))
    } else {
        error.into()
    }
}

fn partition_report(partition: &PartitionPlan) -> PartitionReport {
    PartitionReport {
        partition_id: partition.partition_id.to_string(),
        scope_kind: format!("{:?}", partition.scope.kind()),
        metadata: partition.metadata.clone(),
    }
}

fn preview_resource_report(
    resource: &CliProjectRunSource,
    plan: &EnginePlan,
    schema_snapshot: Option<SchemaSnapshotActionReport>,
    host: &cdf_engine::StandaloneExecutionHost,
) -> cdf_kernel::Result<PreviewReport> {
    let limits = governed_preview_limits(plan.final_limit.or(plan.scan.request.limit))?;
    let preview = host.block_on_root(cdf_engine::preview_resource(
        plan,
        resource.as_project_resource().stream(),
        limits,
    ))?;
    let partition = preview
        .first_partition_id
        .clone()
        .or_else(|| {
            plan.scan
                .inline_partitions()
                .unwrap_or_default()
                .first()
                .map(|partition| partition.partition_id.to_string())
        })
        .unwrap_or_default();
    let batch = preview.first_batch_id.clone().unwrap_or_default();
    let writes = WriteEffects::default();
    Ok(PreviewReport {
        resource: preview.resource_id.to_string(),
        partition: partition.clone(),
        batch: batch.clone(),
        resource_id: preview.resource_id.to_string(),
        batch_id: batch,
        partition_id: partition,
        planned_partition_count: preview.planned_partition_count,
        payload_eligible_partition_count: preview.payload_eligible_partition_count,
        selected_partition_count: preview.selected_partition_count,
        payload_opened_partition_count: preview.payload_opened_partition_count,
        attested_partition_count: preview.attested_partition_count,
        inspected_partition_count: preview.inspected_partition_count,
        partially_inspected_partition_count: preview.partially_inspected_partition_count,
        payload_uninspected_partition_count: preview.payload_uninspected_partition_count,
        inspected_batch_count: preview.inspected_batch_count,
        row_count: preview.row_count,
        byte_count: preview.byte_count,
        output_byte_count: preview.output_byte_count,
        quarantined_row_count: preview.quarantined_row_count,
        residual_row_count: preview.residual_row_count,
        terminal_quarantine_count: preview.terminal_quarantine_count,
        fields: preview.fields,
        limits: preview.limits,
        selection: preview.selection,
        truncated: preview.truncated,
        normalization: plan.validation_program.identifier_policy.clone(),
        schema_snapshot,
        write_effects: writes.clone(),
        writes,
    })
}

fn governed_preview_limits(query_limit: Option<u64>) -> cdf_kernel::Result<EnginePreviewLimits> {
    let defaults = EnginePreviewLimits::default();
    match query_limit {
        Some(limit) if limit > 0 => defaults.clone().with_max_rows(defaults.max_rows.min(limit)),
        Some(0) | None => Ok(defaults),
        Some(_) => unreachable!("positive query limits handled above"),
    }
}

fn lower_runtime_missing(error: &CdfError) -> bool {
    error
        .message
        .contains("execution is outside the MVP compiler crate")
}

#[cfg(test)]
mod render_tests {
    use super::*;

    #[test]
    fn next_run_command_includes_explicit_destination_without_minted_ids() {
        assert_eq!(
            render::next_run_command("local.events", Some("duckdb://.cdf/explain-render.duckdb")),
            "cdf run local.events --to duckdb://.cdf/explain-render.duckdb"
        );
    }

    #[test]
    fn next_run_command_redacts_destination_userinfo() {
        let command = render::next_run_command(
            "local.events",
            Some("postgres://user:secret-value@localhost/db"),
        );

        assert_eq!(
            command,
            "cdf run local.events --to postgres://[redacted]@localhost/db"
        );
        assert!(!command.contains("secret-value"));
        assert!(!command.contains("--package-id"));
        assert!(!command.contains("--checkpoint-id"));
    }

    #[test]
    fn plan_error_wording_uses_plan_command_name() {
        assert_eq!(
            command_correct_scan_message(
                "plan",
                "cdf run requires active state-backed schema authority".to_owned()
            ),
            "cdf plan requires active state-backed schema authority"
        );
    }
}

struct ScanReportPresentation {
    command: &'static str,
    destination_uri: Option<String>,
    schema_snapshot: Option<SchemaSnapshotActionReport>,
    schema_authority: SchemaAuthorityReport,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct PlanReport {
    pub(super) project: String,
    pub(super) environment: String,
    pub(super) selection: cdf_project::ProjectResourceSelection,
    pub(super) counts: PlanCounts,
    pub(super) resources: Vec<PlanResourceOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) artifact: Option<crate::portable_plan_command::PortablePlanWriteReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct PlanCounts {
    pub(super) selected: usize,
    pub(super) ready: usize,
    pub(super) failed: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(super) enum PlanResourceOutcome {
    Ready {
        report: Box<ScanPlanReport>,
    },
    Failed {
        resource_id: String,
        error: PlanResourceError,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct PlanResourceError {
    pub(super) code: String,
    pub(super) kind: cdf_kernel::ErrorKind,
    pub(super) message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(super) suggestions: Vec<String>,
}

impl From<CliError> for PlanResourceError {
    fn from(error: CliError) -> Self {
        Self {
            code: error.code,
            kind: error.kind,
            message: error.message,
            suggestions: error.suggestions.into_vec(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ScanPlanReport {
    #[serde(skip)]
    human_command: &'static str,
    #[serde(skip)]
    human_destination_uri: Option<String>,
    project: String,
    environment: String,
    resource_id: String,
    resource_schema: ResourceSchemaReport,
    normalization: IdentifierPolicy,
    admission: AdmissionPlanReport,
    will_fetch: FetchReport,
    pushdown: PushdownReport,
    destination: DestinationReport,
    ddl_preview: DdlPreviewReport,
    delivery_guarantee: String,
    delivery_guarantee_detail: DeliveryGuaranteeReport,
    state_advancement: StateAdvancementReport,
    explain: cdf_engine::ExplainData,
    #[serde(skip_serializing_if = "Option::is_none")]
    operator_graph: Option<cdf_runtime::CompiledOperatorGraph>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scheduler: Option<cdf_runtime::RuntimeSchedulerResolution>,
    package_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_snapshot: Option<SchemaSnapshotActionReport>,
    schema_authority: SchemaAuthorityReport,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct AdmissionPlanReport {
    dispositions: cdf_contract::AdmissionPolicy,
    observation_strength: &'static str,
    wider_source_observation: bool,
    source_schema_migrations: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaAuthorityReport {
    status: String,
    authority_domain_id: String,
    project_id: String,
    environment: String,
    resource_id: String,
    generation: u64,
    schema_hash: String,
    precondition: cdf_kernel::SchemaAuthorityPrecondition,
    drift: &'static str,
}

impl SchemaAuthorityReport {
    fn from_prepared(
        prepared: &crate::schema_authority::PreparedSchemaAuthority,
    ) -> Result<Self, CliError> {
        let authority = prepared.compiled_authority()?;
        Ok(Self {
            status: prepared.status_name().to_owned(),
            authority_domain_id: authority.key.authority_domain_id.to_string(),
            project_id: authority.key.project_id.to_string(),
            environment: authority.key.environment.to_string(),
            resource_id: authority.key.resource_id.to_string(),
            generation: authority.generation,
            schema_hash: authority.schema_hash.to_string(),
            precondition: prepared.precondition(),
            drift: "none",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ResourceSchemaReport {
    schema_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    baseline_schema_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_schema_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_arrow_schema_hash: Option<String>,
    schema_source: String,
    snapshot_path: Option<String>,
    snapshot_metadata: BTreeMap<String, String>,
    fields: Vec<ResourceSchemaFieldReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ResourceSchemaFieldReport {
    name: String,
    data_type: String,
    nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct FetchReport {
    partition_count: u64,
    partitions: Vec<PartitionReport>,
    projection: Vec<String>,
    filters: Vec<String>,
    limit: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PartitionReport {
    partition_id: String,
    scope_kind: String,
    metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PushdownReport {
    pushed: Vec<cdf_engine::PredicateExplain>,
    inexact: Vec<cdf_engine::PredicateExplain>,
    unsupported: Vec<cdf_engine::PredicateExplain>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DestinationReport {
    destination_id: String,
    schemes: Vec<String>,
    label: String,
    target: String,
    disposition: String,
    idempotency: String,
    supported_dispositions: Vec<String>,
    sheet: DestinationSheet,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DdlPreviewReport {
    supported: bool,
    reason: Option<String>,
    target: String,
    disposition: String,
    migration_support: String,
    migrations: Vec<cdf_kernel::MigrationRecord>,
    synthetic_package_hash: String,
    synthetic_idempotency_token: String,
    synthetic_segments: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DeliveryGuaranteeReport {
    guarantee: String,
    disposition: String,
    idempotency: String,
    qualifier: String,
    basis: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct StateAdvancementReport {
    scope: serde_json::Value,
    cursor: Option<String>,
    advances_after: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PreviewReport {
    resource: String,
    partition: String,
    batch: String,
    resource_id: String,
    batch_id: String,
    partition_id: String,
    planned_partition_count: u64,
    payload_eligible_partition_count: u64,
    selected_partition_count: u64,
    payload_opened_partition_count: u64,
    attested_partition_count: u64,
    inspected_partition_count: u64,
    partially_inspected_partition_count: u64,
    payload_uninspected_partition_count: u64,
    inspected_batch_count: u64,
    row_count: u64,
    byte_count: u64,
    output_byte_count: u64,
    quarantined_row_count: u64,
    residual_row_count: u64,
    terminal_quarantine_count: u64,
    fields: Vec<String>,
    limits: EnginePreviewLimits,
    selection: EnginePreviewSelectionEvidence,
    truncated: bool,
    normalization: IdentifierPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_snapshot: Option<SchemaSnapshotActionReport>,
    write_effects: WriteEffects,
    writes: WriteEffects,
}

struct DestinationPlanReport {
    schema_hash: cdf_kernel::SchemaHash,
    destination: DestinationReport,
    ddl_preview: DdlPreviewReport,
    delivery_guarantee: DeliveryGuaranteeReport,
}

impl DestinationPlanReport {
    fn from_project(
        plan: cdf_project::ProjectDestinationCommitPlan,
        resource: &dyn ResourceStream,
    ) -> cdf_kernel::Result<Self> {
        let guarantee = delivery_guarantee_report(
            &plan.commit_plan.delivery_guarantee,
            &plan.commit_plan.disposition,
            &plan.commit_plan.idempotency,
            &plan.sheet,
            resource,
        )?;
        let migration_support = capability_support_name(&plan.sheet.migration_support).to_owned();
        let ddl_supported = matches!(plan.sheet.migration_support, CapabilitySupport::Supported);
        Ok(Self {
            schema_hash: plan.schema_hash.clone(),
            destination: DestinationReport {
                destination_id: plan.description.destination_id.to_string(),
                schemes: plan
                    .description
                    .schemes
                    .iter()
                    .map(|scheme| (*scheme).to_owned())
                    .collect(),
                label: plan.description.label.clone(),
                target: plan.target.to_string(),
                disposition: write_disposition_name(&plan.commit_plan.disposition).to_owned(),
                idempotency: idempotency_name(&plan.commit_plan.idempotency).to_owned(),
                supported_dispositions: plan
                    .sheet
                    .supported_dispositions
                    .iter()
                    .map(|disposition| write_disposition_name(disposition).to_owned())
                    .collect(),
                sheet: plan.sheet.clone(),
            },
            ddl_preview: DdlPreviewReport {
                supported: ddl_supported,
                reason: if ddl_supported {
                    None
                } else {
                    Some(
                        "destination sheet declares migration_support unsupported; no DDL migration preview is produced for this commit plan"
                            .to_owned(),
                    )
                },
                target: plan.commit_plan.target.to_string(),
                disposition: write_disposition_name(&plan.commit_plan.disposition).to_owned(),
                migration_support,
                migrations: plan.commit_plan.migrations.clone(),
                synthetic_package_hash: plan.synthetic.package_hash.to_string(),
                synthetic_idempotency_token: plan.synthetic.idempotency_token.to_string(),
                synthetic_segments: plan
                    .synthetic
                    .segment_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            },
            delivery_guarantee: guarantee,
        })
    }
}

fn resource_schema_report(
    resource: &dyn ResourceStream,
    schema_hash: &cdf_kernel::SchemaHash,
    output_schema: &arrow_schema::Schema,
    effective: Option<&cdf_engine::EffectiveSchemaPlanEvidence>,
) -> ResourceSchemaReport {
    let snapshot = resource.descriptor().schema_source.cached_snapshot();
    ResourceSchemaReport {
        schema_hash: schema_hash.to_string(),
        baseline_schema_hash: effective
            .map(|evidence| evidence.authority.baseline.schema_hash().to_string()),
        effective_schema_hash: effective
            .map(|evidence| evidence.authority.effective_schema_hash.to_string()),
        effective_arrow_schema_hash: effective
            .map(|evidence| evidence.effective_arrow_schema_hash.to_string()),
        schema_source: schema_source_name(&resource.descriptor().schema_source).to_owned(),
        snapshot_path: snapshot.map(|snapshot| snapshot.path.clone()),
        snapshot_metadata: snapshot
            .map(|snapshot| snapshot.metadata.clone())
            .unwrap_or_default(),
        fields: output_schema
            .fields()
            .iter()
            .map(|field| ResourceSchemaFieldReport {
                name: field.name().clone(),
                data_type: format!("{:?}", field.data_type()),
                nullable: field.is_nullable(),
            })
            .collect(),
    }
}

fn schema_source_name(source: &SchemaSource) -> &'static str {
    match source {
        SchemaSource::Declared { .. } => "declared",
        SchemaSource::Active { .. } => "active",
        SchemaSource::Discover => "discover",
        SchemaSource::Discovered { .. } => "discovered",
        SchemaSource::Hints {
            snapshot: Some(_), ..
        } => "hints_pinned",
        SchemaSource::Hints { snapshot: None, .. } => "hints",
        SchemaSource::Contract { .. } => "contract",
    }
}

fn delivery_guarantee_report(
    planned: &DeliveryGuarantee,
    disposition: &WriteDisposition,
    idempotency: &IdempotencySupport,
    sheet: &DestinationSheet,
    resource: &dyn ResourceStream,
) -> cdf_kernel::Result<DeliveryGuaranteeReport> {
    if idempotency != &sheet.idempotency {
        return Err(CdfError::internal(format!(
            "destination commit plan idempotency {} does not match destination sheet idempotency {}",
            idempotency_name(idempotency),
            idempotency_name(&sheet.idempotency)
        )));
    }
    let expected = derive_delivery_guarantee(disposition, idempotency, sheet, resource);
    if &expected != planned {
        return Err(CdfError::internal(format!(
            "destination commit plan guarantee {} does not match guarantee table result {}",
            delivery_guarantee_name(planned),
            delivery_guarantee_name(&expected)
        )));
    }
    Ok(DeliveryGuaranteeReport {
        guarantee: delivery_guarantee_name(planned).to_owned(),
        disposition: write_disposition_name(disposition).to_owned(),
        idempotency: idempotency_name(idempotency).to_owned(),
        qualifier: delivery_guarantee_qualifier(planned).to_owned(),
        basis: delivery_guarantee_basis(planned).to_owned(),
    })
}

fn derive_delivery_guarantee(
    disposition: &WriteDisposition,
    idempotency: &IdempotencySupport,
    sheet: &DestinationSheet,
    resource: &dyn ResourceStream,
) -> DeliveryGuarantee {
    match disposition {
        WriteDisposition::Merge if !resource.descriptor().merge_key.is_empty() => {
            DeliveryGuarantee::EffectivelyOncePerKey
        }
        WriteDisposition::Append if idempotency == &IdempotencySupport::PackageToken => {
            DeliveryGuarantee::EffectivelyOncePerPackage
        }
        WriteDisposition::Replace
            if matches!(
                sheet.transactions,
                TransactionSupport::AtomicTarget | TransactionSupport::AtomicPackage
            ) =>
        {
            DeliveryGuarantee::EffectivelyOncePerTarget
        }
        WriteDisposition::CdcApply if idempotency == &IdempotencySupport::PackageToken => {
            DeliveryGuarantee::EffectivelyOncePerPosition
        }
        WriteDisposition::Append
        | WriteDisposition::Merge
        | WriteDisposition::Replace
        | WriteDisposition::CdcApply => DeliveryGuarantee::AtLeastOnceDuplicateRisk,
    }
}

fn delivery_guarantee_name(guarantee: &DeliveryGuarantee) -> &'static str {
    match guarantee {
        DeliveryGuarantee::AtLeastOnceDuplicateRisk => "at_least_once_duplicate_risk",
        DeliveryGuarantee::EffectivelyOncePerKey => "effectively_once_per_key",
        DeliveryGuarantee::EffectivelyOncePerPackage => "effectively_once_per_package",
        DeliveryGuarantee::EffectivelyOncePerTarget => "effectively_once_per_target",
        DeliveryGuarantee::EffectivelyOncePerPosition => "effectively_once_per_position",
    }
}

fn delivery_guarantee_qualifier(guarantee: &DeliveryGuarantee) -> &'static str {
    match guarantee {
        DeliveryGuarantee::AtLeastOnceDuplicateRisk => "duplicate_risk",
        DeliveryGuarantee::EffectivelyOncePerKey => "per_key",
        DeliveryGuarantee::EffectivelyOncePerPackage => "per_package",
        DeliveryGuarantee::EffectivelyOncePerTarget => "per_target",
        DeliveryGuarantee::EffectivelyOncePerPosition => "per_position",
    }
}

fn delivery_guarantee_basis(guarantee: &DeliveryGuarantee) -> &'static str {
    match guarantee {
        DeliveryGuarantee::AtLeastOnceDuplicateRisk => {
            "at-least-once extraction without a qualifying idempotent destination rule leaves duplicate risk"
        }
        DeliveryGuarantee::EffectivelyOncePerKey => {
            "at-least-once extraction plus merge with a merge key gives effectively-once per key"
        }
        DeliveryGuarantee::EffectivelyOncePerPackage => {
            "at-least-once extraction plus append with package-token idempotency gives effectively-once per package"
        }
        DeliveryGuarantee::EffectivelyOncePerTarget => {
            "at-least-once extraction plus atomic replace gives effectively-once per target"
        }
        DeliveryGuarantee::EffectivelyOncePerPosition => {
            "at-least-once extraction plus ordered cdc_apply with package-token idempotency gives effectively-once per position"
        }
    }
}

fn write_disposition_name(disposition: &WriteDisposition) -> &'static str {
    match disposition {
        WriteDisposition::Append => "append",
        WriteDisposition::Replace => "replace",
        WriteDisposition::Merge => "merge",
        WriteDisposition::CdcApply => "cdc_apply",
    }
}

fn idempotency_name(idempotency: &IdempotencySupport) -> &'static str {
    match idempotency {
        IdempotencySupport::None => "none",
        IdempotencySupport::PackageToken => "package_token",
        IdempotencySupport::SegmentToken => "segment_token",
    }
}

fn capability_support_name(support: &CapabilitySupport) -> &'static str {
    match support {
        CapabilitySupport::Supported => "supported",
        CapabilitySupport::Unsupported => "unsupported",
    }
}

#[cfg(test)]
mod source_authority_tests {
    use std::collections::BTreeMap;

    use super::{governed_preview_limits, validate_recorded_source_authority};
    use cdf_kernel::SourceDiscoveryBinding;

    #[test]
    fn locked_schema_rejects_a_different_compiled_source_plan() {
        let metadata = BTreeMap::from([
            ("source_driver".to_owned(), "files".to_owned()),
            ("source_driver_version".to_owned(), "1.0.0".to_owned()),
            (
                "source_discovery_binding".to_owned(),
                format!("sha256:{}", "a".repeat(64)),
            ),
        ]);
        let recompiled = SourceDiscoveryBinding::new(format!("sha256:{}", "b".repeat(64)))
            .expect("valid discovery binding");
        let error = validate_recorded_source_authority(&metadata, "files", "1.0.0", &recompiled)
            .unwrap_err();
        assert!(
            error
                .message
                .contains("active schema observation provenance")
        );
        assert!(error.message.contains("does not match source authority"));
        assert!(error.message.contains("cdf schema diff"));
        assert!(error.message.contains("promote or recompile"));
    }

    #[test]
    fn query_limits_can_only_tighten_the_governed_preview_bound() {
        let defaults = governed_preview_limits(None).unwrap();
        assert_eq!(governed_preview_limits(Some(0)).unwrap(), defaults);
        assert_eq!(
            governed_preview_limits(Some(defaults.max_rows + 1))
                .unwrap()
                .max_rows,
            defaults.max_rows
        );
        assert_eq!(governed_preview_limits(Some(7)).unwrap().max_rows, 7);
    }
}
