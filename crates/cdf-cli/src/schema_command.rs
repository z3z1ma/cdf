mod render;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use cdf_declarative::CompiledResource;
use cdf_kernel::{
    CdfError, EnvironmentName, LeaseOwnerId, PipelineId, ResourceId, SchemaAuthorityKey,
    SchemaAuthorityStore, SchemaHead, SchemaHeadStatus, SchemaPromotionLifecyclePhase,
    SchemaSnapshotReference, SchemaSource, SchemaVersion, TargetName,
};
use cdf_project::{
    DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS, PromotionEvidenceInventory,
    ResourceSchemaDiscoveryArtifacts, SchemaDiscoveryExecutionOptions,
    SchemaPromotionExecutionRequest, SchemaPromotionPlanReport, SchemaPromotionPlanningAuthority,
    SchemaSnapshotArtifact, SchemaSnapshotDataType, SchemaSnapshotField, SchemaSnapshotSchema,
    SchemaSnapshotStore, execute_schema_promotion,
};
use cdf_state_sqlite::{
    SqliteSchemaAuthorityState, SqliteSchemaAuthorityStore, SqliteSchemaPromotionStore,
};
use serde::Serialize;

use crate::{
    args::{Cli, SchemaCommand, SchemaPromoteArgs, SchemaResourceArgs},
    context::ProjectContext,
    destination_uri::{redact_error_value, resolve_selected_destination_with_services},
    output::{CliError, CommandOutput},
    reports::DiscoveryCoverageReport,
};

pub(crate) fn schema(
    cli: &Cli,
    command: SchemaCommand,
    execution: &cdf_runtime::ExecutionServices,
    destination_registry: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    match command {
        SchemaCommand::Show(args) => show(cli, args, destination_registry),
        SchemaCommand::Diff(args) => diff(cli, args, execution, destination_registry),
        SchemaCommand::Promote(args) => promote(cli, args, execution, destination_registry),
    }
}

fn promote(
    cli: &Cli,
    args: SchemaPromoteArgs,
    execution: &cdf_runtime::ExecutionServices,
    destination_registry: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let context = load_context(cli, "schema promote", destination_registry)?;
    let resource = context.resource(&args.resource_id)?;
    if args.execute {
        return execute_promotion(&context, resource, &args, execution, destination_registry);
    }
    let prepared = prepare_new_promotion(
        &context,
        resource,
        &args.types,
        execution,
        destination_registry,
        "schema-promote",
    )?;
    CommandOutput::rendered(
        "schema promote",
        render::schema_promote_document(&prepared.report),
        prepared.report,
    )
}

fn execute_promotion(
    context: &ProjectContext,
    resource: &CompiledResource,
    args: &SchemaPromoteArgs,
    execution: &cdf_runtime::ExecutionServices,
    destination_registry: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let resource_id = ResourceId::new(args.resource_id.clone())?;
    let prepared = if let Some((head, version, report)) =
        load_resumable_promotion_state(context, &resource_id, &args.types)?
    {
        let expected_types = report
            .paths
            .iter()
            .filter_map(|path| {
                path.selected_type
                    .as_ref()
                    .map(|data_type| format!("{}={data_type}", path.path))
            })
            .collect::<std::collections::BTreeSet<_>>();
        let supplied_types = args
            .types
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        if !supplied_types.is_empty() && supplied_types != expected_types {
            return Err(CdfError::contract(
                "schema promote recovery --type values conflict with the exact state-backed plan; use the rendered recovery command",
            )
            .into());
        }
        prepare_resumable_promotion(
            context,
            resource,
            head,
            version,
            report,
            execution,
            destination_registry,
        )?
    } else {
        prepare_new_promotion(
            context,
            resource,
            &args.types,
            execution,
            destination_registry,
            "schema-promote-execute",
        )?
    };

    let state_path = context.state_store_path()?;
    let settlement_store = SqliteSchemaPromotionStore::open_with_path_ownership(
        &state_path,
        context.state_store_path_ownership(),
    )?;
    let result = execute_schema_promotion(SchemaPromotionExecutionRequest {
        project_root: &context.root,
        package_root: &context.package_root(),
        resource,
        authority: &prepared.authority,
        dry_plan: &prepared.report,
        destinations: prepared.destinations,
        execution_services: execution.clone(),
        pipeline_id: PipelineId::new("cdf-schema-promotion")?,
        lease_owner: LeaseOwnerId::new(format!("schema-promote:{}", prepared.report.promotion_id))?,
        lease_duration_ms: DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS,
        settlement_store: &settlement_store,
        failpoint: None,
    })
    .map_err(|mut error| {
        for redaction in prepared.redactions.iter().flatten() {
            error = redact_error_value(error, Some(redaction));
        }
        let mut cli_error = CliError::from(error);
        if let Ok(promotion_id) = cdf_kernel::PromotionId::new(prepared.report.promotion_id.clone())
            && let Ok(Some(state)) =
                settlement_store.promotion_state(&prepared.authority.head.key, &promotion_id)
            && let Ok(mut details) = serde_json::to_value(&state)
        {
            if let Some(details) = details.as_object_mut() {
                details.insert(
                    "remaining_action".to_owned(),
                    serde_json::Value::String(promotion_remaining_action(&state.phase).to_owned()),
                );
                details.insert(
                    "recovery_command".to_owned(),
                    serde_json::Value::String(format!(
                        "{} --execute",
                        prepared.report.recovery_command
                    )),
                );
            }
            cli_error = cli_error.with_details(details);
        }
        cli_error
    })?;
    CommandOutput::rendered(
        "schema promote",
        render::schema_promotion_execution_document(&result),
        result,
    )
}

fn promotion_remaining_action(phase: &SchemaPromotionLifecyclePhase) -> &'static str {
    match phase {
        SchemaPromotionLifecyclePhase::Fenced => "establish the promotion cutoff",
        SchemaPromotionLifecyclePhase::CutoffEstablished => {
            "build authenticated correction packages"
        }
        SchemaPromotionLifecyclePhase::Published => "none",
    }
}

struct PreparedPromotion {
    authority: SchemaPromotionPlanningAuthority,
    report: SchemaPromotionPlanReport,
    destinations: Vec<cdf_project::ResolvedProjectDestination>,
    redactions: Vec<Option<String>>,
}

fn prepare_new_promotion(
    context: &ProjectContext,
    resource: &CompiledResource,
    types: &[String],
    execution: &cdf_runtime::ExecutionServices,
    destination_registry: &cdf_runtime::DestinationRegistry,
    inspection_label: &str,
) -> Result<PreparedPromotion, CliError> {
    let resource_id = resource.descriptor().resource_id.clone();
    let active = crate::schema_authority::load_active(context, &resource_id)?.ok_or_else(|| {
        CdfError::contract(format!(
            "schema promote requires active state authority for `{resource_id}`; run `cdf compile {resource_id}` first"
        ))
    })?;
    let inventory =
        cdf_project::LocalPackagePromotionEvidenceInventory::new(context.package_root());
    let facts = inventory.inventory(resource_id.as_str())?;
    let targets = facts
        .paths
        .iter()
        .flat_map(|path| path.associations.iter())
        .map(|association| (association.destination.clone(), association.target.clone()))
        .collect::<BTreeSet<_>>();
    let (authority, destinations, redactions) = resolve_promotion_authority(
        context,
        resource,
        active.head,
        active.version,
        targets,
        execution,
        destination_registry,
    )?;
    let inspection_root = inspection_artifact_root(inspection_label)?;
    let fresh_discovery =
        match discover_artifacts_for_cli_at(context, resource, execution, inspection_root.path()) {
            Ok(artifacts) => cdf_project::SchemaPromotionFreshDiscovery::Available {
                content_identity: artifacts.discovery.snapshot.source_identity,
                snapshot: Box::new(artifacts.discovery.snapshot.artifact),
                discovery_manifest: artifacts.discovery_manifest.map(Box::new),
            },
            Err(error) => cdf_project::SchemaPromotionFreshDiscovery::Unavailable {
                reason: error.message,
            },
        };
    let report = cdf_project::plan_schema_promotion(
        &inventory,
        resource,
        &authority,
        &fresh_discovery,
        types,
    )?;
    Ok(PreparedPromotion {
        authority,
        report,
        destinations,
        redactions,
    })
}

fn prepare_resumable_promotion(
    context: &ProjectContext,
    resource: &CompiledResource,
    head: SchemaHead,
    version: SchemaVersion,
    report: SchemaPromotionPlanReport,
    execution: &cdf_runtime::ExecutionServices,
    destination_registry: &cdf_runtime::DestinationRegistry,
) -> Result<PreparedPromotion, CliError> {
    let targets = report
        .targets
        .iter()
        .map(|target| (target.destination.clone(), target.target.clone()))
        .collect();
    let (authority, destinations, redactions) = resolve_promotion_authority(
        context,
        resource,
        head,
        version,
        targets,
        execution,
        destination_registry,
    )?;
    cdf_project::validate_schema_promotion_plan_identity(&report, &authority)?;
    Ok(PreparedPromotion {
        authority,
        report,
        destinations,
        redactions,
    })
}

type PromotionAuthorityResolution = (
    SchemaPromotionPlanningAuthority,
    Vec<cdf_project::ResolvedProjectDestination>,
    Vec<Option<String>>,
);

fn resolve_promotion_authority(
    context: &ProjectContext,
    resource: &CompiledResource,
    head: SchemaHead,
    version: SchemaVersion,
    targets: BTreeSet<(String, String)>,
    execution: &cdf_runtime::ExecutionServices,
    destination_registry: &cdf_runtime::DestinationRegistry,
) -> Result<PromotionAuthorityResolution, CliError> {
    let schema_cache = SchemaSnapshotArtifact::new(
        &resource.descriptor().resource_id,
        &version.canonical_schema.to_arrow()?,
        BTreeMap::new(),
    )?;
    let mut destination_sheets = BTreeMap::new();
    let mut destinations = Vec::new();
    let mut redactions = Vec::new();
    for (expected_destination, target) in targets {
        let target = TargetName::new(target)?;
        let resolved = resolve_selected_destination_with_services(
            destination_registry,
            context,
            &target,
            None,
            Some(execution),
        )
        .map_err(|error| CliError::from(redact_error_value(error, None)))?;
        let actual_destination = resolved.destination.describe().destination_id.to_string();
        if actual_destination != expected_destination {
            return Err(CdfError::contract(format!(
                "resolved destination {actual_destination} does not match promotion target {expected_destination} for {target}"
            ))
            .into());
        }
        let sheet = resolved.destination.destination_sheet_artifact()?;
        if destination_sheets
            .insert(actual_destination.clone(), sheet.clone())
            .is_some_and(|existing| existing != sheet)
        {
            return Err(CdfError::contract(format!(
                "destination {actual_destination} returned inconsistent capability authority while planning promotion"
            ))
            .into());
        }
        redactions.push(resolved.secret_redaction);
        destinations.push(resolved.destination);
    }
    let authority = SchemaPromotionPlanningAuthority {
        head,
        version,
        schema_cache,
        destinations: destination_sheets,
    };
    authority.validate(resource)?;
    Ok((authority, destinations, redactions))
}

fn load_resumable_promotion_state(
    context: &ProjectContext,
    resource_id: &ResourceId,
    supplied_types: &[String],
) -> Result<Option<(SchemaHead, SchemaVersion, SchemaPromotionPlanReport)>, CliError> {
    let state_path = context.state_store_path()?;
    let ownership = context.state_store_path_ownership();
    let SqliteSchemaAuthorityState::Ready {
        authority_domain_id,
    } = SqliteSchemaAuthorityStore::inspect_state(&state_path, ownership)?
    else {
        return Ok(None);
    };
    let key = SchemaAuthorityKey::new(
        authority_domain_id,
        context.config.project.id.clone(),
        EnvironmentName::new(context.environment.name.clone())?,
        resource_id.clone(),
    )?;
    let store = SqliteSchemaPromotionStore::open_with_path_ownership(&state_path, ownership)?;
    let Some(current_head) = SchemaAuthorityStore::head(&store, &key)? else {
        return Ok(None);
    };
    let (promotion_id, from_schema_hash, published) = match &current_head.status {
        SchemaHeadStatus::Promoting {
            promotion_id,
            from_schema_hash,
            ..
        } => (promotion_id.clone(), from_schema_hash.clone(), false),
        SchemaHeadStatus::Active if !supplied_types.is_empty() => {
            let Some(event) = SchemaAuthorityStore::history(&store, &key, 1)?.pop() else {
                return Ok(None);
            };
            let cdf_kernel::SchemaAuthorityEventKind::PromotionPublished {
                promotion_id,
                from_schema_hash,
                ..
            } = event.kind
            else {
                return Ok(None);
            };
            (promotion_id, from_schema_hash, true)
        }
        SchemaHeadStatus::Active => return Ok(None),
    };
    let state = store
        .promotion_state(&key, &promotion_id)?
        .ok_or_else(|| CdfError::internal("promoting schema head has no lifecycle state"))?;
    let version = store
        .version(&key, &from_schema_hash)?
        .ok_or_else(|| CdfError::internal("promoting schema head has no source version"))?;
    let report: SchemaPromotionPlanReport = serde_json::from_str(&state.plan.canonical_plan_json)
        .map_err(|error| {
        CdfError::internal(format!(
            "decode state-backed schema promotion plan: {error}"
        ))
    })?;
    if published {
        let expected_types = report
            .paths
            .iter()
            .filter_map(|path| {
                path.selected_type
                    .as_ref()
                    .map(|data_type| format!("{}={data_type}", path.path))
            })
            .collect::<BTreeSet<_>>();
        if expected_types != supplied_types.iter().cloned().collect::<BTreeSet<_>>() {
            return Ok(None);
        }
    }
    let active_head = SchemaHead::active(key, state.from_generation, state.from_schema_hash)?;
    Ok(Some((active_head, version, report)))
}

fn show(
    cli: &Cli,
    args: SchemaResourceArgs,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let context = load_context(cli, "schema show", destinations)?;
    context.resource(&args.resource_id)?;
    let resource_id = ResourceId::new(args.resource_id.clone())?;
    let active = crate::schema_authority::load_active(&context, &resource_id)?
        .ok_or_else(|| no_active_schema_authority_error(&args.resource_id))?;
    let report = SchemaShowReport::from_authority(&context, &active)?;
    CommandOutput::rendered("schema show", render::schema_show_document(&report), report)
}

fn diff(
    cli: &Cli,
    args: SchemaResourceArgs,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let context = load_context(cli, "schema diff", destinations)?;
    let resource = context.resource(&args.resource_id)?;
    let resource_id = ResourceId::new(args.resource_id.clone())?;
    let active = crate::schema_authority::load_active(&context, &resource_id)?
        .ok_or_else(|| no_active_schema_authority_error(&args.resource_id))?;
    let active_schema = active.version.canonical_schema.to_arrow()?;
    let source_schema = arrow_schema::Schema::new_with_metadata(
        active_schema
            .fields()
            .iter()
            .filter(|field| !cdf_contract::is_framework_variant_field(field.as_ref()))
            .cloned()
            .collect::<Vec<_>>(),
        active_schema.metadata().clone(),
    );
    let baseline = SchemaSnapshotSchema::from_arrow(&source_schema);
    let probe_resource =
        resource.with_schema_source_and_schema(SchemaSource::Discover, Arc::new(source_schema));
    let inspection_root = inspection_artifact_root("schema-diff")?;
    let artifacts = discover_artifacts_for_cli_resource(
        &context,
        &probe_resource,
        Default::default(),
        execution,
        inspection_root.path(),
    )?;
    let fresh = &artifacts.discovery.snapshot.artifact;
    let report = SchemaDiffReport::from_snapshots(
        &context,
        &args.resource_id,
        &active.head,
        &baseline,
        fresh,
        artifacts.discovery_manifest.as_ref(),
    );
    CommandOutput::rendered("schema diff", render::schema_diff_document(&report), report)
}

fn load_context(
    cli: &Cli,
    _command: &str,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<ProjectContext, CliError> {
    ProjectContext::load_for_command_with_destination_registry(
        cli.project.as_ref(),
        cli.env.as_deref(),
        destinations,
    )
}

fn discover_artifacts_for_cli_at(
    context: &ProjectContext,
    resource: &CompiledResource,
    execution: &cdf_runtime::ExecutionServices,
    artifact_root: &std::path::Path,
) -> Result<ResourceSchemaDiscoveryArtifacts, CliError> {
    let cached = cached_snapshot_reference(resource).cloned();
    if let Some(snapshot) = cached {
        let (baseline, verified_baseline) =
            SchemaSnapshotStore::new(&context.root).read_with_verified_baseline(&snapshot)?;
        let probe_resource = resource.with_schema_source_and_schema(
            SchemaSource::Discover,
            Arc::new(baseline.schema.to_arrow()?),
        );
        return discover_artifacts_for_cli_resource(
            context,
            &probe_resource,
            SchemaDiscoveryExecutionOptions::new().with_verified_baseline(verified_baseline),
            execution,
            artifact_root,
        );
    }
    if matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
        return discover_artifacts_for_cli_resource(
            context,
            resource,
            Default::default(),
            execution,
            artifact_root,
        );
    }
    discover_artifacts_for_cli_resource(
        context,
        resource,
        Default::default(),
        execution,
        artifact_root,
    )
}

fn discover_artifacts_for_cli_resource(
    context: &ProjectContext,
    resource: &CompiledResource,
    options: SchemaDiscoveryExecutionOptions,
    execution: &cdf_runtime::ExecutionServices,
    artifact_root: &std::path::Path,
) -> Result<ResourceSchemaDiscoveryArtifacts, CliError> {
    let options =
        options.with_observation_cache(cdf_project::ObservationCacheStore::new(artifact_root));
    let source_plan = crate::project_run_resource::compile_source_plan_for_cli(resource)?;
    Ok(
        crate::project_run_resource::discover_source_schema_with_plan_for_cli_at(
            context,
            resource,
            &source_plan,
            execution,
            cdf_runtime::PreparedSourcePayloads::default(),
            options,
            artifact_root,
        )?,
    )
}

fn inspection_artifact_root(command: &str) -> Result<tempfile::TempDir, CliError> {
    tempfile::Builder::new()
        .prefix(&format!("cdf-{command}-"))
        .tempdir()
        .map_err(|error| {
            CdfError::environment(format!(
                "create {command} inspection artifact root in the host temporary directory: {error}; check temporary-directory access, free space, and process file limits before retrying"
            ))
            .into()
        })
}

fn cached_snapshot_reference(resource: &CompiledResource) -> Option<&SchemaSnapshotReference> {
    resource.descriptor().schema_source.cached_snapshot()
}

fn no_active_schema_authority_error(resource_id: &str) -> CliError {
    CliError::from(CdfError::contract(format!(
        "resource `{resource_id}` has no active state-backed schema authority; run `cdf compile {resource_id}` to establish it"
    )))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaShowReport {
    #[serde(flatten)]
    snapshot: SchemaSnapshotReportBase,
    #[serde(skip_serializing_if = "Option::is_none")]
    discovery: Option<DiscoveryCoverageReport>,
    writes: SchemaWrites,
    next_command: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaSnapshotReportBase {
    project: String,
    environment: String,
    resource_id: String,
    authority_domain: String,
    generation: u64,
    schema_hash: String,
    provenance: String,
    created_at_ms: i64,
    fields: Vec<SchemaFieldReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaDiffReport {
    project: String,
    environment: String,
    resource_id: String,
    authority_domain: String,
    authority_generation: u64,
    active_schema_hash: String,
    fresh_schema_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    discovery: Option<DiscoveryCoverageReport>,
    summary: SchemaDiffSummary,
    added_fields: Vec<SchemaFieldReport>,
    removed_fields: Vec<SchemaFieldReport>,
    type_changed_fields: Vec<SchemaFieldValueChange<SchemaSnapshotDataType>>,
    nullable_changed_fields: Vec<SchemaFieldValueChange<bool>>,
    metadata_changed_fields: Vec<SchemaFieldMetadataChange>,
    writes: SchemaWrites,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaDiffSummary {
    changed: bool,
    added_fields: usize,
    removed_fields: usize,
    type_changed_fields: usize,
    nullable_changed_fields: usize,
    metadata_changed_fields: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaFieldReport {
    name: String,
    data_type: SchemaSnapshotDataType,
    nullable: bool,
    source_name: Option<String>,
    metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaFieldValueChange<T> {
    name: String,
    before: T,
    after: T,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaFieldMetadataChange {
    name: String,
    before: BTreeMap<String, String>,
    after: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaWrites {
    schema_snapshot: bool,
    package: bool,
    destination: bool,
    checkpoint: bool,
}

impl SchemaShowReport {
    fn from_authority(
        context: &ProjectContext,
        active: &crate::schema_authority::ActiveSchemaAuthority,
    ) -> Result<Self, CliError> {
        Ok(Self {
            snapshot: SchemaSnapshotReportBase::from_authority(context, active)?,
            discovery: None,
            writes: SchemaWrites::none(),
            next_command: format!("cdf schema diff {}", active.head.key.resource_id),
        })
    }
}

impl SchemaSnapshotReportBase {
    fn from_authority(
        context: &ProjectContext,
        active: &crate::schema_authority::ActiveSchemaAuthority,
    ) -> Result<Self, CliError> {
        let schema = SchemaSnapshotSchema::from_arrow(&active.version.canonical_schema.to_arrow()?);
        Ok(Self {
            project: context.config.project.name.clone(),
            environment: context.environment.name.clone(),
            resource_id: active.head.key.resource_id.to_string(),
            authority_domain: active.head.key.authority_domain_id.to_string(),
            generation: active.head.generation,
            schema_hash: active.head.schema_hash.to_string(),
            provenance: match &active.version.provenance {
                cdf_kernel::SchemaVersionProvenance::FirstUse => "first_use".to_owned(),
                cdf_kernel::SchemaVersionProvenance::Promotion { promotion_id } => {
                    format!("promotion:{promotion_id}")
                }
            },
            created_at_ms: active.version.created_at_ms,
            fields: field_reports(&schema.fields),
        })
    }
}

impl SchemaDiffReport {
    fn from_snapshots(
        context: &ProjectContext,
        resource_id: &str,
        active: &SchemaHead,
        baseline: &SchemaSnapshotSchema,
        fresh: &SchemaSnapshotArtifact,
        manifest: Option<&cdf_project::DiscoveryManifestArtifact>,
    ) -> Self {
        let active_fields = fields_by_name(&baseline.fields);
        let fresh_fields = fields_by_name(&fresh.schema.fields);

        let added_fields = fresh_fields
            .iter()
            .filter(|(name, _)| !active_fields.contains_key(*name))
            .map(|(_, field)| SchemaFieldReport::from_field(field))
            .collect::<Vec<_>>();
        let removed_fields = active_fields
            .iter()
            .filter(|(name, _)| !fresh_fields.contains_key(*name))
            .map(|(_, field)| SchemaFieldReport::from_field(field))
            .collect::<Vec<_>>();
        let mut type_changed_fields = Vec::new();
        let mut nullable_changed_fields = Vec::new();
        let mut metadata_changed_fields = Vec::new();
        for (name, active_field) in &active_fields {
            let Some(fresh_field) = fresh_fields.get(name) else {
                continue;
            };
            if active_field.data_type != fresh_field.data_type {
                type_changed_fields.push(SchemaFieldValueChange {
                    name: (*name).clone(),
                    before: active_field.data_type.clone(),
                    after: fresh_field.data_type.clone(),
                });
            }
            if active_field.nullable != fresh_field.nullable {
                nullable_changed_fields.push(SchemaFieldValueChange {
                    name: (*name).clone(),
                    before: active_field.nullable,
                    after: fresh_field.nullable,
                });
            }
            if active_field.metadata != fresh_field.metadata {
                metadata_changed_fields.push(SchemaFieldMetadataChange {
                    name: (*name).clone(),
                    before: active_field.metadata.clone(),
                    after: fresh_field.metadata.clone(),
                });
            }
        }
        let summary = SchemaDiffSummary {
            changed: !added_fields.is_empty()
                || !removed_fields.is_empty()
                || !type_changed_fields.is_empty()
                || !nullable_changed_fields.is_empty()
                || !metadata_changed_fields.is_empty(),
            added_fields: added_fields.len(),
            removed_fields: removed_fields.len(),
            type_changed_fields: type_changed_fields.len(),
            nullable_changed_fields: nullable_changed_fields.len(),
            metadata_changed_fields: metadata_changed_fields.len(),
        };
        Self {
            project: context.config.project.name.clone(),
            environment: context.environment.name.clone(),
            resource_id: resource_id.to_owned(),
            authority_domain: active.key.authority_domain_id.to_string(),
            authority_generation: active.generation,
            active_schema_hash: active.schema_hash.to_string(),
            fresh_schema_hash: fresh.schema_hash.to_string(),
            discovery: manifest.map(DiscoveryCoverageReport::from_manifest),
            summary,
            added_fields,
            removed_fields,
            type_changed_fields,
            nullable_changed_fields,
            metadata_changed_fields,
            writes: SchemaWrites::none(),
        }
    }
}

impl SchemaFieldReport {
    fn from_field(field: &SchemaSnapshotField) -> Self {
        Self {
            name: field.name.clone(),
            data_type: field.data_type.clone(),
            nullable: field.nullable,
            source_name: field.metadata.get("cdf:source_name").cloned(),
            metadata: field.metadata.clone(),
        }
    }
}

impl SchemaWrites {
    fn none() -> Self {
        Self {
            schema_snapshot: false,
            package: false,
            destination: false,
            checkpoint: false,
        }
    }
}

fn field_reports(fields: &[SchemaSnapshotField]) -> Vec<SchemaFieldReport> {
    fields.iter().map(SchemaFieldReport::from_field).collect()
}

fn fields_by_name(fields: &[SchemaSnapshotField]) -> BTreeMap<String, &SchemaSnapshotField> {
    fields
        .iter()
        .map(|field| (field.name.clone(), field))
        .collect()
}
