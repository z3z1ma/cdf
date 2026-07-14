use std::{collections::BTreeMap, fs, sync::Arc};

use cdf_declarative::{CompiledResource, CompiledResourcePlan};
use cdf_kernel::{
    CdfError, LeaseOwnerId, PipelineId, PromotionId, ResourceId, SchemaSnapshotReference,
    SchemaSource, TargetName,
};
use cdf_project::{
    DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS, DiscoveryManifestStore, LOCK_FILE_NAME,
    ResourceSchemaDiscoveryArtifacts, SchemaDiscoveryExecutionOptions,
    SchemaPromotionExecutionRequest, SchemaSnapshotArtifact, SchemaSnapshotDataType,
    SchemaSnapshotField, SchemaSnapshotStore, execute_schema_promotion,
    load_resumable_schema_promotion, load_schema_promotion_recovery_status, lock_to_toml,
    parse_lock, pin_schema_snapshot_in_project_lockfile, write_schema_discovery_artifacts,
};
use cdf_state_sqlite::SqlitePromotionSettlementStore;
use serde::Serialize;

use crate::{
    args::{Cli, SchemaCommand, SchemaDiscoverArgs, SchemaPromoteArgs, SchemaResourceArgs},
    context::ProjectContext,
    destination_uri::{redact_error_value, resolve_selected_destination_with_services},
    http_transport::ReqwestHttpTransport,
    output::{CliError, CommandOutput},
    project_run_resource::file_runtime_dependencies,
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
    },
    reports::{DiscoveryCoverageReport, discovery_coverage_panel},
};

pub(crate) fn schema(
    cli: &Cli,
    command: SchemaCommand,
    execution: &cdf_runtime::ExecutionServices,
    destination_registry: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    match command {
        SchemaCommand::Discover(args) => discover(cli, args, execution),
        SchemaCommand::Pin(args) => pin(cli, args, execution, destination_registry),
        SchemaCommand::Show(args) => show(cli, args),
        SchemaCommand::Diff(args) => diff(cli, args, execution),
        SchemaCommand::Promote(args) => promote(cli, args, execution, destination_registry),
    }
}

fn promote(
    cli: &Cli,
    args: SchemaPromoteArgs,
    execution: &cdf_runtime::ExecutionServices,
    destination_registry: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let context = load_context(cli, "schema promote")?;
    let resource = context.resource(&args.resource_id)?;
    if args.execute {
        return execute_promotion(&context, resource, &args, execution, destination_registry);
    }
    let reference = pinned_snapshot_reference(&context, resource)
        .ok_or_else(|| no_pinned_snapshot_error(&args.resource_id))?;
    let pinned = SchemaSnapshotStore::new(&context.root).read(reference)?;
    let lock = context.lock.as_ref().ok_or_else(|| {
        CdfError::contract("schema promote requires cdf.lock; run `cdf schema pin` first")
    })?;
    let authority = context.lock_authority.as_ref().ok_or_else(|| {
        CdfError::contract("schema promote requires an exact cdf.lock precondition")
    })?;
    let fresh_discovery = match discover_artifacts_for_cli(&context, resource, execution) {
        Ok(artifacts) => cdf_project::SchemaPromotionFreshDiscovery::Available {
            content_identity: artifacts.discovery.snapshot.source_identity,
            snapshot: Box::new(artifacts.discovery.snapshot.artifact),
            discovery_manifest: artifacts.discovery_manifest.map(Box::new),
        },
        Err(error) => cdf_project::SchemaPromotionFreshDiscovery::Unavailable {
            reason: error.message,
        },
    };
    let evidence_inventory =
        cdf_project::LocalPackagePromotionEvidenceInventory::new(context.package_root());
    let report = cdf_project::plan_schema_promotion(
        &evidence_inventory,
        resource,
        &pinned,
        lock,
        authority,
        &fresh_discovery,
        &args.types,
    )?;
    CommandOutput::rendered("schema promote", schema_promote_document(&report), report)
}

fn execute_promotion(
    context: &ProjectContext,
    resource: &CompiledResource,
    args: &SchemaPromoteArgs,
    execution: &cdf_runtime::ExecutionServices,
    destination_registry: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let current_authority = context.lock_authority.as_ref().ok_or_else(|| {
        CdfError::contract("schema promote --execute requires an exact cdf.lock precondition")
    })?;
    let resource_id = ResourceId::new(args.resource_id.clone())?;
    let resumable =
        load_resumable_schema_promotion(&context.root, &resource_id, current_authority)?;
    let (lock, authority, report) = if let Some(staged) = resumable {
        let expected_types = staged
            .dry_plan
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
                "schema promote recovery --type values conflict with the exact staged authority; use the rendered recovery command",
            )
            .into());
        }
        let lock = parse_lock(
            std::str::from_utf8(&staged.old_lock_authority.bytes)
                .map_err(|error| CdfError::data(error.to_string()))?,
        )?;
        (lock, staged.old_lock_authority, staged.dry_plan)
    } else {
        let reference = pinned_snapshot_reference(context, resource)
            .ok_or_else(|| no_pinned_snapshot_error(&args.resource_id))?;
        let pinned = SchemaSnapshotStore::new(&context.root).read(reference)?;
        let lock = context.lock.as_ref().ok_or_else(|| {
            CdfError::contract("schema promote requires cdf.lock; run `cdf schema pin` first")
        })?;
        let fresh_discovery = match discover_artifacts_for_cli(context, resource, execution) {
            Ok(artifacts) => cdf_project::SchemaPromotionFreshDiscovery::Available {
                content_identity: artifacts.discovery.snapshot.source_identity,
                snapshot: Box::new(artifacts.discovery.snapshot.artifact),
                discovery_manifest: artifacts.discovery_manifest.map(Box::new),
            },
            Err(error) => cdf_project::SchemaPromotionFreshDiscovery::Unavailable {
                reason: error.message,
            },
        };
        let inventory =
            cdf_project::LocalPackagePromotionEvidenceInventory::new(context.package_root());
        let report = cdf_project::plan_schema_promotion(
            &inventory,
            resource,
            &pinned,
            lock,
            current_authority,
            &fresh_discovery,
            &args.types,
        )?;
        (lock.clone(), current_authority.clone(), report)
    };

    let mut destinations = Vec::new();
    let mut redactions = Vec::new();
    for target in &report.targets {
        let target_name = TargetName::new(target.target.clone())?;
        let resolved = resolve_selected_destination_with_services(
            destination_registry,
            context,
            &target_name,
            None,
            Some(execution),
        )
        .map_err(|error| CliError::from(redact_error_value(error, None)))?;
        if resolved.destination.describe().destination_id.as_str() != target.destination {
            return Err(CdfError::contract(format!(
                "resolved destination {} does not match staged promotion target {} for {}",
                resolved.destination.describe().destination_id,
                target.destination,
                target.target
            ))
            .into());
        }
        redactions.push(resolved.secret_redaction);
        destinations.push(resolved.destination);
    }

    let state_path = context.state_store_path()?;
    let settlement_store = SqlitePromotionSettlementStore::open(&state_path)?;
    let result = execute_schema_promotion(SchemaPromotionExecutionRequest {
        project_root: &context.root,
        package_root: &context.package_root(),
        resource,
        lock: &lock,
        lock_authority: &authority,
        dry_plan: &report,
        destinations,
        pipeline_id: PipelineId::new("cdf-schema-promotion")?,
        lease_owner: LeaseOwnerId::new(format!("schema-promote:{}", report.promotion_id))?,
        lease_duration_ms: DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS,
        settlement_store: &settlement_store,
        failpoint: None,
    })
    .map_err(|mut error| {
        for redaction in redactions.iter().flatten() {
            error = redact_error_value(error, Some(redaction));
        }
        let mut cli_error = CliError::from(error);
        if let Ok(promotion_id) = PromotionId::new(report.promotion_id.clone())
            && let Ok(Some(status)) =
                load_schema_promotion_recovery_status(&context.root, &promotion_id)
            && let Ok(details) = serde_json::to_value(status)
        {
            cli_error = cli_error.with_details(details);
        }
        cli_error
    })?;
    CommandOutput::rendered(
        "schema promote",
        schema_promotion_execution_document(&result),
        result,
    )
}

fn discover(
    cli: &Cli,
    args: SchemaDiscoverArgs,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<CommandOutput, CliError> {
    let context = load_context(cli, "schema discover")?;
    let resource = context.resource(&args.resource_id)?;
    let artifacts = discover_artifacts_for_cli(&context, resource, execution)?;
    let discovery = &artifacts.discovery;
    let report = SchemaDiscoverReport::from_discovery(
        &context,
        &args.resource_id,
        &discovery.snapshot.artifact,
        &discovery.snapshot.source_identity,
        artifacts.discovery_manifest.as_ref(),
    );
    CommandOutput::rendered("schema discover", schema_discover_document(&report), report)
}

fn pin(
    cli: &Cli,
    args: SchemaResourceArgs,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let context = load_context(cli, "schema pin")?;
    let resource = context.resource(&args.resource_id)?;
    let previous = pinned_snapshot_reference(&context, resource).cloned();
    let previous_artifact = previous
        .as_ref()
        .map(|reference| SchemaSnapshotStore::new(&context.root).read(reference))
        .transpose()?;
    let artifacts = discover_artifacts_for_cli(&context, resource, execution)?;
    let unchanged = previous_artifact
        .as_ref()
        .zip(artifacts.discovery_manifest.as_ref())
        .map(|(previous_snapshot, fresh_manifest)| {
            has_same_discovery_observation(&context, previous_snapshot, fresh_manifest)
        })
        .transpose()?
        .unwrap_or(false);
    let (snapshot, normalized_schema, snapshot_written) = if unchanged {
        let previous_snapshot = previous_artifact.as_ref().ok_or_else(|| {
            CdfError::internal("unchanged schema pin lost its verified previous snapshot")
        })?;
        (
            previous_snapshot,
            Arc::new(previous_snapshot.schema.to_arrow()?),
            false,
        )
    } else {
        let writes = write_schema_discovery_artifacts(&context.root, &artifacts)?;
        (
            &artifacts.discovery.snapshot.artifact,
            Arc::clone(&artifacts.discovery.normalized_schema),
            writes.snapshot_written,
        )
    };
    let pinned_source = resource
        .descriptor()
        .schema_source
        .with_pinned_snapshot(snapshot.reference())
        .ok_or_else(|| {
            CdfError::contract(format!(
                "resource `{}` does not support schema pinning",
                resource.descriptor().resource_id
            ))
        })?;
    let pinned_resource = resource.with_schema_source_and_schema(pinned_source, normalized_schema);
    let lockfile = update_lockfile(destinations, &context, &pinned_resource)?;
    let status = match previous {
        Some(_) if unchanged => "unchanged",
        Some(previous) if previous.schema_hash == snapshot.schema_hash => "unchanged",
        Some(_) => "refreshed",
        None => "added",
    };
    let report = SchemaPinReport::from_pin(
        SchemaSnapshotReportBase::from_artifact(&context, &args.resource_id, snapshot),
        status,
        &artifacts.discovery.snapshot.source_identity,
        snapshot_written,
        lockfile,
        artifacts.discovery_manifest.as_ref(),
    );
    CommandOutput::rendered("schema pin", schema_pin_document(&report), report)
}

fn show(cli: &Cli, args: SchemaResourceArgs) -> Result<CommandOutput, CliError> {
    let context = load_context(cli, "schema show")?;
    let resource = context.resource(&args.resource_id)?;
    let reference = pinned_snapshot_reference(&context, resource)
        .ok_or_else(|| no_pinned_snapshot_error(&args.resource_id))?;
    let artifact = SchemaSnapshotStore::new(&context.root).read(reference)?;
    let manifest = artifact
        .discovery_manifest_reference()?
        .map(|reference| DiscoveryManifestStore::new(&context.root).read(&reference))
        .transpose()?;
    let report =
        SchemaShowReport::from_artifact(&context, &args.resource_id, &artifact, manifest.as_ref());
    CommandOutput::rendered("schema show", schema_show_document(&report), report)
}

fn diff(
    cli: &Cli,
    args: SchemaResourceArgs,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<CommandOutput, CliError> {
    let context = load_context(cli, "schema diff")?;
    let resource = context.resource(&args.resource_id)?;
    let reference = pinned_snapshot_reference(&context, resource)
        .ok_or_else(|| no_pinned_snapshot_error(&args.resource_id))?;
    let pinned = SchemaSnapshotStore::new(&context.root).read(reference)?;
    let artifacts = discover_artifacts_for_cli(&context, resource, execution)?;
    let unchanged = artifacts
        .discovery_manifest
        .as_ref()
        .map(|fresh_manifest| has_same_discovery_observation(&context, &pinned, fresh_manifest))
        .transpose()?
        .unwrap_or(false);
    let fresh = if unchanged {
        &pinned
    } else {
        &artifacts.discovery.snapshot.artifact
    };
    let report = SchemaDiffReport::from_snapshots(
        &context,
        &args.resource_id,
        &pinned,
        fresh,
        artifacts.discovery_manifest.as_ref(),
    );
    CommandOutput::rendered("schema diff", schema_diff_document(&report), report)
}

fn has_same_discovery_observation(
    context: &ProjectContext,
    previous_snapshot: &SchemaSnapshotArtifact,
    fresh_manifest: &cdf_project::DiscoveryManifestArtifact,
) -> Result<bool, CliError> {
    let previous_manifest = previous_snapshot
        .discovery_manifest_reference()?
        .map(|reference| DiscoveryManifestStore::new(&context.root).read(&reference))
        .transpose()?;
    Ok(previous_manifest
        .as_ref()
        .is_some_and(|manifest| manifest.has_same_observation(fresh_manifest)))
}

fn load_context(cli: &Cli, command: &str) -> Result<ProjectContext, CliError> {
    ProjectContext::load_for_command(command, cli.project.as_ref(), cli.env.as_deref())
}

fn discover_artifacts_for_cli(
    context: &ProjectContext,
    resource: &CompiledResource,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<ResourceSchemaDiscoveryArtifacts, CliError> {
    let pinned = pinned_snapshot_reference(context, resource).cloned();
    if let Some(snapshot) = pinned {
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
        );
    }
    if matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
        return discover_artifacts_for_cli_resource(
            context,
            resource,
            Default::default(),
            execution,
        );
    }
    discover_artifacts_for_cli_resource(context, resource, Default::default(), execution)
}

fn discover_artifacts_for_cli_resource(
    context: &ProjectContext,
    resource: &CompiledResource,
    options: SchemaDiscoveryExecutionOptions,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<ResourceSchemaDiscoveryArtifacts, CliError> {
    let options =
        options.with_observation_cache(cdf_project::ObservationCacheStore::new(&context.root));
    let secret_provider = context.secret_provider();
    if matches!(resource.descriptor().schema_source, SchemaSource::Discover)
        && matches!(resource.plan(), CompiledResourcePlan::Files(_))
    {
        return Ok(
            cdf_project::discover_resource_schema_with_file_dependencies_artifacts(
                resource,
                &secret_provider,
                file_runtime_dependencies(context, Some(execution))?,
                options,
            )?,
        );
    }
    if matches!(resource.descriptor().schema_source, SchemaSource::Discover)
        && matches!(resource.plan(), CompiledResourcePlan::Rest(_))
    {
        let transport = ReqwestHttpTransport::new()?;
        Ok(ResourceSchemaDiscoveryArtifacts::new(
            cdf_project::discover_resource_schema_with_rest_transport(
                resource,
                &secret_provider,
                &transport,
            )?,
            None,
        ))
    } else {
        Ok(cdf_project::discover_resource_schema_artifacts(
            resource,
            &secret_provider,
            options,
        )?)
    }
}

fn update_lockfile(
    destinations: &cdf_runtime::DestinationRegistry,
    context: &ProjectContext,
    pinned_resource: &CompiledResource,
) -> Result<SchemaLockfileWrite, CliError> {
    let destination_artifacts = crate::destination_registry::inspect_destination_artifacts(
        destinations,
        context,
        &context.environment.destination,
    )?;
    let updated = pin_schema_snapshot_in_project_lockfile(
        &context.config,
        &context.resources,
        context.lock.as_ref(),
        &destination_artifacts,
        pinned_resource,
    )?;
    let encoded = lock_to_toml(&updated)?;
    let path = context.root.join(LOCK_FILE_NAME);
    let written = fs::read_to_string(&path).ok().as_deref() != Some(&encoded);
    if written {
        cdf_project::write_lock_file_guarded(&path, context.lock_authority.as_ref(), encoded)?;
    }
    Ok(SchemaLockfileWrite {
        written,
        unsupported_reason: None,
    })
}

fn pinned_snapshot_reference<'a>(
    context: &'a ProjectContext,
    resource: &'a CompiledResource,
) -> Option<&'a SchemaSnapshotReference> {
    resource
        .descriptor()
        .schema_source
        .pinned_snapshot()
        .or_else(|| {
            context
                .lock
                .as_ref()
                .and_then(|lock| {
                    lock.resources
                        .get(resource.descriptor().resource_id.as_str())
                })
                .and_then(|locked| locked.schema_snapshot.as_ref())
        })
}

fn no_pinned_snapshot_error(resource_id: &str) -> CliError {
    CliError::from(CdfError::contract(format!(
        "no pinned schema snapshot exists for resource `{resource_id}`; run `cdf schema pin {resource_id}` to create one"
    )))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaDiscoverReport {
    #[serde(flatten)]
    snapshot: SchemaSnapshotReportBase,
    source_identity: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    discovery: Option<DiscoveryCoverageReport>,
    writes: SchemaWrites,
    next_command: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaPinReport {
    #[serde(flatten)]
    snapshot: SchemaSnapshotReportBase,
    status: String,
    source_identity: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    discovery: Option<DiscoveryCoverageReport>,
    writes: SchemaWrites,
    unsupported: Vec<String>,
    next_command: String,
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
    schema_hash: String,
    schema_snapshot_path: String,
    snapshot_metadata: BTreeMap<String, String>,
    fields: Vec<SchemaFieldReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaDiffReport {
    project: String,
    environment: String,
    resource_id: String,
    pinned_schema_hash: String,
    fresh_schema_hash: String,
    pinned_schema_snapshot_path: String,
    fresh_schema_snapshot_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    discovery: Option<DiscoveryCoverageReport>,
    summary: SchemaDiffSummary,
    added_fields: Vec<SchemaFieldReport>,
    removed_fields: Vec<SchemaFieldReport>,
    type_changed_fields: Vec<SchemaFieldValueChange<SchemaSnapshotDataType>>,
    nullable_changed_fields: Vec<SchemaFieldValueChange<bool>>,
    metadata_changed_fields: Vec<SchemaFieldMetadataChange>,
    snapshot_metadata_changed: Vec<SchemaMetadataChange>,
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
    snapshot_metadata_changed: usize,
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
struct SchemaMetadataChange {
    key: String,
    before: Option<String>,
    after: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaWrites {
    schema_snapshot: bool,
    lockfile: bool,
    package: bool,
    destination: bool,
    checkpoint: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SchemaLockfileWrite {
    written: bool,
    unsupported_reason: Option<String>,
}

impl SchemaDiscoverReport {
    fn from_discovery(
        context: &ProjectContext,
        resource_id: &str,
        artifact: &SchemaSnapshotArtifact,
        source_identity: &BTreeMap<String, String>,
        manifest: Option<&cdf_project::DiscoveryManifestArtifact>,
    ) -> Self {
        Self {
            snapshot: SchemaSnapshotReportBase::from_artifact(context, resource_id, artifact),
            source_identity: source_identity.clone(),
            discovery: manifest.map(DiscoveryCoverageReport::from_manifest),
            writes: SchemaWrites::none(),
            next_command: format!("cdf plan {resource_id}"),
        }
    }
}

impl SchemaPinReport {
    fn from_pin(
        snapshot: SchemaSnapshotReportBase,
        status: &str,
        source_identity: &BTreeMap<String, String>,
        snapshot_written: bool,
        lockfile: SchemaLockfileWrite,
        manifest: Option<&cdf_project::DiscoveryManifestArtifact>,
    ) -> Self {
        let unsupported = lockfile.unsupported_reason.into_iter().collect::<Vec<_>>();
        let resource_id = snapshot.resource_id.clone();
        Self {
            snapshot,
            status: status.to_owned(),
            source_identity: source_identity.clone(),
            discovery: manifest.map(DiscoveryCoverageReport::from_manifest),
            writes: SchemaWrites {
                schema_snapshot: snapshot_written,
                lockfile: lockfile.written,
                package: false,
                destination: false,
                checkpoint: false,
            },
            unsupported,
            next_command: format!("cdf schema show {resource_id}"),
        }
    }
}

impl SchemaShowReport {
    fn from_artifact(
        context: &ProjectContext,
        resource_id: &str,
        artifact: &SchemaSnapshotArtifact,
        manifest: Option<&cdf_project::DiscoveryManifestArtifact>,
    ) -> Self {
        Self {
            snapshot: SchemaSnapshotReportBase::from_artifact(context, resource_id, artifact),
            discovery: manifest.map(DiscoveryCoverageReport::from_manifest),
            writes: SchemaWrites::none(),
            next_command: format!("cdf schema diff {resource_id}"),
        }
    }
}

impl SchemaSnapshotReportBase {
    fn from_artifact(
        context: &ProjectContext,
        resource_id: &str,
        artifact: &SchemaSnapshotArtifact,
    ) -> Self {
        Self {
            project: context.config.project.name.clone(),
            environment: context.environment.name.clone(),
            resource_id: resource_id.to_owned(),
            schema_hash: artifact.schema_hash.to_string(),
            schema_snapshot_path: artifact.path.clone(),
            snapshot_metadata: artifact.metadata.clone(),
            fields: field_reports(&artifact.schema.fields),
        }
    }
}

impl SchemaDiffReport {
    fn from_snapshots(
        context: &ProjectContext,
        resource_id: &str,
        pinned: &SchemaSnapshotArtifact,
        fresh: &SchemaSnapshotArtifact,
        manifest: Option<&cdf_project::DiscoveryManifestArtifact>,
    ) -> Self {
        let pinned_fields = fields_by_name(&pinned.schema.fields);
        let fresh_fields = fields_by_name(&fresh.schema.fields);

        let added_fields = fresh_fields
            .iter()
            .filter(|(name, _)| !pinned_fields.contains_key(*name))
            .map(|(_, field)| SchemaFieldReport::from_field(field))
            .collect::<Vec<_>>();
        let removed_fields = pinned_fields
            .iter()
            .filter(|(name, _)| !fresh_fields.contains_key(*name))
            .map(|(_, field)| SchemaFieldReport::from_field(field))
            .collect::<Vec<_>>();
        let mut type_changed_fields = Vec::new();
        let mut nullable_changed_fields = Vec::new();
        let mut metadata_changed_fields = Vec::new();
        for (name, pinned_field) in &pinned_fields {
            let Some(fresh_field) = fresh_fields.get(name) else {
                continue;
            };
            if pinned_field.data_type != fresh_field.data_type {
                type_changed_fields.push(SchemaFieldValueChange {
                    name: (*name).clone(),
                    before: pinned_field.data_type.clone(),
                    after: fresh_field.data_type.clone(),
                });
            }
            if pinned_field.nullable != fresh_field.nullable {
                nullable_changed_fields.push(SchemaFieldValueChange {
                    name: (*name).clone(),
                    before: pinned_field.nullable,
                    after: fresh_field.nullable,
                });
            }
            if pinned_field.metadata != fresh_field.metadata {
                metadata_changed_fields.push(SchemaFieldMetadataChange {
                    name: (*name).clone(),
                    before: pinned_field.metadata.clone(),
                    after: fresh_field.metadata.clone(),
                });
            }
        }
        let snapshot_metadata_changed = metadata_changes(&pinned.metadata, &fresh.metadata);
        let summary = SchemaDiffSummary {
            changed: !added_fields.is_empty()
                || !removed_fields.is_empty()
                || !type_changed_fields.is_empty()
                || !nullable_changed_fields.is_empty()
                || !metadata_changed_fields.is_empty()
                || !snapshot_metadata_changed.is_empty(),
            added_fields: added_fields.len(),
            removed_fields: removed_fields.len(),
            type_changed_fields: type_changed_fields.len(),
            nullable_changed_fields: nullable_changed_fields.len(),
            metadata_changed_fields: metadata_changed_fields.len(),
            snapshot_metadata_changed: snapshot_metadata_changed.len(),
        };
        Self {
            project: context.config.project.name.clone(),
            environment: context.environment.name.clone(),
            resource_id: resource_id.to_owned(),
            pinned_schema_hash: pinned.schema_hash.to_string(),
            fresh_schema_hash: fresh.schema_hash.to_string(),
            pinned_schema_snapshot_path: pinned.path.clone(),
            fresh_schema_snapshot_path: fresh.path.clone(),
            discovery: manifest.map(DiscoveryCoverageReport::from_manifest),
            summary,
            added_fields,
            removed_fields,
            type_changed_fields,
            nullable_changed_fields,
            metadata_changed_fields,
            snapshot_metadata_changed,
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
            lockfile: false,
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

fn metadata_changes(
    before: &BTreeMap<String, String>,
    after: &BTreeMap<String, String>,
) -> Vec<SchemaMetadataChange> {
    let mut keys = before.keys().chain(after.keys()).collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter_map(|key| {
            let before_value = before.get(key).cloned();
            let after_value = after.get(key).cloned();
            (before_value != after_value).then(|| SchemaMetadataChange {
                key: key.clone(),
                before: before_value,
                after: after_value,
            })
        })
        .collect()
}

fn schema_discover_document(report: &SchemaDiscoverReport) -> RenderDocument {
    schema_snapshot_document(
        "discovered",
        &format!("discovered schema for {}", report.snapshot.resource_id),
        SnapshotDocumentData {
            base: &report.snapshot,
            writes: &report.writes,
            source_identity: Some(&report.source_identity),
            discovery: report.discovery.as_ref(),
            unsupported: &[],
            next_command: Some(&report.next_command),
        },
    )
}

fn schema_pin_document(report: &SchemaPinReport) -> RenderDocument {
    schema_snapshot_document(
        "pinned",
        &format!(
            "{} pinned schema for {}",
            report.status, report.snapshot.resource_id
        ),
        SnapshotDocumentData {
            base: &report.snapshot,
            writes: &report.writes,
            source_identity: Some(&report.source_identity),
            discovery: report.discovery.as_ref(),
            unsupported: &report.unsupported,
            next_command: Some(&report.next_command),
        },
    )
}

fn schema_show_document(report: &SchemaShowReport) -> RenderDocument {
    schema_snapshot_document(
        "pinned",
        &format!("showing pinned schema for {}", report.snapshot.resource_id),
        SnapshotDocumentData {
            base: &report.snapshot,
            writes: &report.writes,
            source_identity: None,
            discovery: report.discovery.as_ref(),
            unsupported: &[],
            next_command: Some(&report.next_command),
        },
    )
}

struct SnapshotDocumentData<'a> {
    base: &'a SchemaSnapshotReportBase,
    writes: &'a SchemaWrites,
    source_identity: Option<&'a BTreeMap<String, String>>,
    discovery: Option<&'a DiscoveryCoverageReport>,
    unsupported: &'a [String],
    next_command: Option<&'a str>,
}

fn schema_snapshot_document(
    label: &str,
    status: &str,
    data: SnapshotDocumentData<'_>,
) -> RenderDocument {
    let mut document = RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(StatusKind::Success, status))
        .blank_line()
        .push(
            KeyValuePanel::new("Schema")
                .row("project", data.base.project.clone())
                .row("environment", data.base.environment.clone())
                .row("resource", data.base.resource_id.clone())
                .row("state", label.to_owned())
                .row("hash", data.base.schema_hash.clone())
                .row("path", data.base.schema_snapshot_path.clone())
                .row(
                    "probe",
                    data.base
                        .snapshot_metadata
                        .get("probe")
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_owned()),
                )
                .row(
                    "normalizer",
                    data.base
                        .snapshot_metadata
                        .get("cdf:normalizer")
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_owned()),
                ),
        )
        .blank_line()
        .push(field_table(&data.base.fields));

    if let Some(source_identity) = data.source_identity {
        document = document
            .blank_line()
            .push(key_value_table("Source Identity", source_identity));
    }
    if let Some(discovery) = data.discovery {
        document = document
            .blank_line()
            .push(discovery_coverage_panel(discovery));
    }
    document = document.blank_line().push(writes_panel(data.writes));
    if !data.unsupported.is_empty() {
        document = document.blank_line().push(
            data.unsupported
                .iter()
                .fold(KeyValuePanel::new("Unsupported"), |panel, reason| {
                    panel.row("lockfile reference", reason.clone())
                }),
        );
    }
    if let Some(next_command) = data.next_command {
        document = document
            .blank_line()
            .push(NextCommand::new(next_command.to_owned()));
    }
    document
}

fn schema_diff_document(report: &SchemaDiffReport) -> RenderDocument {
    let status = if report.summary.changed {
        StatusKind::Warning
    } else {
        StatusKind::Success
    };
    let document = RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            status,
            if report.summary.changed {
                format!("schema drift detected for {}", report.resource_id)
            } else {
                format!("schema matches fresh probe for {}", report.resource_id)
            },
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Schema Diff")
                .row("project", report.project.clone())
                .row("environment", report.environment.clone())
                .row("resource", report.resource_id.clone())
                .row("pinned hash", report.pinned_schema_hash.clone())
                .row("fresh hash", report.fresh_schema_hash.clone())
                .row("pinned path", report.pinned_schema_snapshot_path.clone())
                .row(
                    "fresh candidate path",
                    report.fresh_schema_snapshot_path.clone(),
                ),
        )
        .blank_line()
        .push(
            KeyValuePanel::new("Summary")
                .row("added fields", report.summary.added_fields.to_string())
                .row("removed fields", report.summary.removed_fields.to_string())
                .row(
                    "type changes",
                    report.summary.type_changed_fields.to_string(),
                )
                .row(
                    "nullable changes",
                    report.summary.nullable_changed_fields.to_string(),
                )
                .row(
                    "metadata changes",
                    report.summary.metadata_changed_fields.to_string(),
                )
                .row(
                    "snapshot metadata changes",
                    report.summary.snapshot_metadata_changed.to_string(),
                ),
        )
        .blank_line()
        .push(diff_table(report));
    let document = if let Some(discovery) = &report.discovery {
        document
            .blank_line()
            .push(discovery_coverage_panel(discovery))
    } else {
        document
    };
    document.blank_line().push(writes_panel(&report.writes))
}

fn schema_promotion_execution_document(
    report: &cdf_project::SchemaPromotionExecutionReport,
) -> RenderDocument {
    let targets = report.targets.iter().fold(
        Table::new([
            "destination",
            "target",
            "package",
            "receipt",
            "checkpoint",
            "committed",
        ]),
        |table, target| {
            table.row([
                target.destination.clone(),
                target.target.clone(),
                target.correction_package_hash.clone(),
                target
                    .receipt_id
                    .clone()
                    .unwrap_or_else(|| "pending".to_owned()),
                target
                    .checkpoint_id
                    .clone()
                    .unwrap_or_else(|| "pending".to_owned()),
                yes_no(target.committed).to_owned(),
            ])
        },
    );
    RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            format!("schema promotion complete for {}", report.resource_id),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Promotion execution")
                .row("promotion", report.promotion_id.clone())
                .row("phase", format!("{:?}", report.phase).to_lowercase())
                .row("resumed", yes_no(report.resumed))
                .row("old schema", report.old_schema_hash.clone())
                .row("new schema", report.new_schema_hash.clone())
                .row("staged plan", report.staged_plan_path.clone())
                .row("snapshot", report.snapshot_path.clone())
                .row("lock published", yes_no(report.lock_published))
                .row(
                    "publication event",
                    yes_no(report.publication_event_recorded),
                )
                .row("remaining action", report.remaining_action.clone()),
        )
        .blank_line()
        .push(targets)
        .blank_line()
        .push(NextCommand::new(report.recovery_command.clone()))
}

fn schema_promote_document(report: &cdf_project::SchemaPromotionPlanReport) -> RenderDocument {
    let mut document = RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            if report.executable {
                StatusKind::Success
            } else {
                StatusKind::Warning
            },
            format!(
                "promotion plan {} for {}",
                if report.executable {
                    "ready"
                } else {
                    "blocked"
                },
                report.resource_id
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Promotion")
                .row("id", report.promotion_id.clone())
                .row("old schema", report.old_schema_hash.clone())
                .row(
                    "new schema",
                    report
                        .new_schema_hash
                        .clone()
                        .unwrap_or_else(|| "blocked".to_owned()),
                )
                .row(
                    "snapshot path",
                    report
                        .new_schema_snapshot_path
                        .clone()
                        .unwrap_or_else(|| "blocked".to_owned()),
                )
                .row(
                    "fresh discovery",
                    report
                        .fresh_discovery_schema_hash
                        .clone()
                        .unwrap_or_else(|| "unavailable".to_owned()),
                )
                .row(
                    "discovery coverage",
                    report
                        .fresh_discovery_file_coverage
                        .as_ref()
                        .map(|coverage| {
                            match coverage {
                                cdf_project::DiscoveryFileCoverage::AllFiles => "all_files",
                                cdf_project::DiscoveryFileCoverage::SampledFiles => "sampled_files",
                            }
                            .to_owned()
                        })
                        .unwrap_or_else(|| "unavailable".to_owned()),
                )
                .row(
                    "discovery manifest",
                    report
                        .fresh_discovery_manifest_hash
                        .clone()
                        .unwrap_or_else(|| "unavailable".to_owned()),
                )
                .row("lock precondition", report.lock_precondition_sha256.clone()),
        );
    if !report.fresh_discovery_content_identity.is_empty() {
        document = document
            .blank_line()
            .push(report.fresh_discovery_content_identity.iter().fold(
                KeyValuePanel::new("Fresh discovery identity"),
                |panel, (key, value)| panel.row(key, value),
            ));
    }
    let mut table = Table::new(["path", "source", "observed", "count", "selected", "output"]);
    let mut path_evidence = Table::new(["path", "coercions", "packages", "address examples"]);
    for path in &report.paths {
        table = table.row([
            path.path.clone(),
            path.source_name.clone(),
            path.observed_types.join(", "),
            path.observed_count.to_string(),
            path.selected_type
                .clone()
                .unwrap_or_else(|| "required".to_owned()),
            path.output_field.clone(),
        ]);
        path_evidence = path_evidence.row([
            path.path.clone(),
            path.coercion_verdicts
                .iter()
                .map(|verdict| {
                    format!(
                        "{}→{}:{}",
                        verdict.observed_type.to_arrow().map_or_else(
                            |_| "invalid".to_owned(),
                            |data_type| data_type.to_string()
                        ),
                        verdict.selected_type.to_arrow().map_or_else(
                            |_| "invalid".to_owned(),
                            |data_type| data_type.to_string()
                        ),
                        promotion_coercion_label(verdict.decision)
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
            path.affected_packages.join(", "),
            path.affected_row_examples
                .iter()
                .map(|address| {
                    format!(
                        "{}/{}/{}",
                        address.original_package_hash,
                        address.original_segment_id,
                        address.original_row_ordinal
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        ]);
    }
    document = document
        .blank_line()
        .push(table)
        .blank_line()
        .push(path_evidence);
    let mut evidence = Table::new(["package", "availability", "rows", "receipts"]);
    for item in &report.evidence {
        evidence = evidence.row([
            item.package_hash
                .clone()
                .unwrap_or_else(|| item.artifact_location.clone()),
            promotion_availability_label(&item.availability).to_owned(),
            item.residual_rows.to_string(),
            item.recorded_receipts.len().to_string(),
        ]);
    }
    document = document.blank_line().push(evidence);
    let mut targets = Table::new(["destination", "target", "strategy", "migrations"]);
    for target in &report.targets {
        targets = targets.row([
            target.destination.clone(),
            target.target.clone(),
            target
                .strategy
                .map(promotion_strategy_label)
                .unwrap_or_else(|| "unsupported".to_owned()),
            target.migrations.len().to_string(),
        ]);
    }
    document = document.blank_line().push(targets);
    for target in &report.targets {
        document = document.blank_line().push(
            KeyValuePanel::new(format!(
                "Target evidence {}:{}",
                target.destination, target.target
            ))
            .row("sheet hash", target.destination_sheet_hash.clone())
            .row(
                "receipt verification",
                promotion_receipt_verification_label(&target.receipt_verification),
            )
            .row("receipts", target.recorded_receipt_ids.join(", "))
            .row("packages", target.affected_packages.join(", "))
            .row("paths", target.affected_paths.join(", "))
            .row(
                "evidence availability",
                target
                    .evidence
                    .iter()
                    .map(|item| {
                        format!(
                            "{}:{}",
                            item.package_hash,
                            promotion_availability_label(&item.availability)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
        );
    }
    let mut migrations = Table::new([
        "target",
        "path",
        "output",
        "destination field",
        "mapping",
        "fidelity",
    ]);
    for target in &report.targets {
        for migration in &target.migrations {
            migrations = migrations.row([
                format!("{}:{}", target.destination, target.target),
                migration.path.clone(),
                migration.output_field.clone(),
                migration
                    .destination_field
                    .clone()
                    .unwrap_or_else(|| "blocked".to_owned()),
                format!(
                    "{} -> {}",
                    migration.arrow_type,
                    migration
                        .destination_type
                        .as_deref()
                        .unwrap_or("unsupported")
                ),
                migration
                    .fidelity
                    .as_ref()
                    .map(promotion_fidelity_label)
                    .unwrap_or("missing")
                    .to_owned(),
            ]);
        }
    }
    document = document.blank_line().push(migrations);
    let evidence_details = report.evidence.iter().filter_map(|item| {
        item.detail.as_ref().map(|detail| {
            (
                item.package_hash
                    .as_deref()
                    .unwrap_or(&item.artifact_location),
                detail.as_str(),
            )
        })
    });
    let mut details_panel = KeyValuePanel::new("Evidence constraints");
    let mut has_evidence_details = false;
    for (location, detail) in evidence_details {
        has_evidence_details = true;
        details_panel = details_panel.row(location, detail);
    }
    if has_evidence_details {
        document = document.blank_line().push(details_panel);
    }
    document = document.blank_line().push(
        KeyValuePanel::new("Writes")
            .row("snapshot", yes_no(report.writes.schema_snapshot))
            .row("lockfile", yes_no(report.writes.lockfile))
            .row("package", yes_no(report.writes.package))
            .row("destination", yes_no(report.writes.destination))
            .row("checkpoint", yes_no(report.writes.checkpoint))
            .row("lease", yes_no(report.writes.lease))
            .row("ledger", yes_no(report.writes.ledger)),
    );
    if !report.conflicts.is_empty() {
        document = document.blank_line().push(report.conflicts.iter().fold(
            KeyValuePanel::new("Conflicts"),
            |panel, conflict| {
                panel.row(
                    &conflict.code,
                    format!("{} Fix: {}", conflict.message, conflict.remediation),
                )
            },
        ));
    }
    if !report.execution_preconditions.is_empty() {
        document =
            document
                .blank_line()
                .push(report.execution_preconditions.iter().enumerate().fold(
                    KeyValuePanel::new("Execution preconditions"),
                    |panel, (index, precondition)| {
                        panel.row(format!("{}", index + 1), precondition)
                    },
                ));
    }
    document
        .blank_line()
        .push(NextCommand::new(report.recovery_command.clone()))
}

fn promotion_availability_label(
    availability: &cdf_project::SchemaPromotionEvidenceAvailability,
) -> &'static str {
    match availability {
        cdf_project::SchemaPromotionEvidenceAvailability::RetainedPackage => "retained_package",
        cdf_project::SchemaPromotionEvidenceAvailability::DestinationReadback => {
            "destination_readback"
        }
        cdf_project::SchemaPromotionEvidenceAvailability::TombstoneOnly => "tombstone_only",
        cdf_project::SchemaPromotionEvidenceAvailability::Missing => "missing",
    }
}

fn promotion_strategy_label(strategy: cdf_kernel::CorrectionStrategy) -> String {
    match strategy {
        cdf_kernel::CorrectionStrategy::InPlaceUpdate => "in_place_update",
        cdf_kernel::CorrectionStrategy::CorrectionSidecar => "correction_sidecar",
        cdf_kernel::CorrectionStrategy::VersionedRematerialization => "versioned_rematerialization",
    }
    .to_owned()
}

fn promotion_fidelity_label(fidelity: &cdf_kernel::TypeMappingFidelity) -> &'static str {
    match fidelity {
        cdf_kernel::TypeMappingFidelity::Lossless => "lossless",
        cdf_kernel::TypeMappingFidelity::LossyRequiresContractAllowance => {
            "lossy_requires_contract_allowance"
        }
        cdf_kernel::TypeMappingFidelity::Unsupported => "unsupported",
    }
}

fn promotion_receipt_verification_label(
    verification: &cdf_project::SchemaPromotionReceiptVerification,
) -> &'static str {
    match verification {
        cdf_project::SchemaPromotionReceiptVerification::StructuralCoverageVerifiedDestinationVerificationPending => {
            "structural_coverage_verified_destination_verification_pending"
        }
    }
}

fn promotion_coercion_label(decision: cdf_contract::FieldCoercionDecision) -> &'static str {
    match decision {
        cdf_contract::FieldCoercionDecision::Preserved => "preserved",
        cdf_contract::FieldCoercionDecision::Widened => "widened",
        cdf_contract::FieldCoercionDecision::CoercedByPolicy => "coerced_by_policy",
        cdf_contract::FieldCoercionDecision::LossyAllowed => "lossy_allowed",
        cdf_contract::FieldCoercionDecision::LossyRejected => "lossy_rejected",
        cdf_contract::FieldCoercionDecision::Unsupported => "unsupported",
        cdf_contract::FieldCoercionDecision::Missing => "missing",
        cdf_contract::FieldCoercionDecision::Extra => "extra",
    }
}

fn diff_table(report: &SchemaDiffReport) -> Table {
    let mut table = Table::new(["kind", "field", "before", "after"]);
    for field in &report.added_fields {
        table = table.row([
            "added".to_owned(),
            field.name.clone(),
            String::new(),
            format!("{:?}", field.data_type),
        ]);
    }
    for field in &report.removed_fields {
        table = table.row([
            "removed".to_owned(),
            field.name.clone(),
            format!("{:?}", field.data_type),
            String::new(),
        ]);
    }
    for change in &report.type_changed_fields {
        table = table.row([
            "type".to_owned(),
            change.name.clone(),
            format!("{:?}", change.before),
            format!("{:?}", change.after),
        ]);
    }
    for change in &report.nullable_changed_fields {
        table = table.row([
            "nullable".to_owned(),
            change.name.clone(),
            yes_no(change.before).to_owned(),
            yes_no(change.after).to_owned(),
        ]);
    }
    for change in &report.metadata_changed_fields {
        table = table.row([
            "metadata".to_owned(),
            change.name.clone(),
            metadata_keys(&change.before),
            metadata_keys(&change.after),
        ]);
    }
    for change in &report.snapshot_metadata_changed {
        table = table.row([
            "snapshot metadata".to_owned(),
            change.key.clone(),
            change.before.clone().unwrap_or_default(),
            change.after.clone().unwrap_or_default(),
        ]);
    }
    table
}

fn field_table(fields: &[SchemaFieldReport]) -> Table {
    fields.iter().fold(
        Table::new(["field", "type", "nullable", "source"]),
        |table, field| {
            table.row([
                field.name.clone(),
                format!("{:?}", field.data_type),
                yes_no(field.nullable).to_owned(),
                field
                    .source_name
                    .clone()
                    .unwrap_or_else(|| field.name.clone()),
            ])
        },
    )
}

fn key_value_table(title: &str, values: &BTreeMap<String, String>) -> KeyValuePanel {
    values
        .iter()
        .fold(KeyValuePanel::new(title), |panel, (key, value)| {
            panel.row(key.clone(), value.clone())
        })
}

fn writes_panel(writes: &SchemaWrites) -> KeyValuePanel {
    KeyValuePanel::new("Writes")
        .row("schema snapshot", yes_no(writes.schema_snapshot))
        .row("lockfile", yes_no(writes.lockfile))
        .row("package", yes_no(writes.package))
        .row("destination", yes_no(writes.destination))
        .row("checkpoint", yes_no(writes.checkpoint))
}

fn metadata_keys(metadata: &BTreeMap<String, String>) -> String {
    metadata.keys().cloned().collect::<Vec<_>>().join(", ")
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
