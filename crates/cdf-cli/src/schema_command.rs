mod render;

use std::{collections::BTreeMap, sync::Arc};

use cdf_declarative::CompiledResource;
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
    let inspection_root = inspection_artifact_root("schema-promote")?;
    let fresh_discovery = match discover_artifacts_for_cli_at(
        &context,
        resource,
        execution,
        inspection_root.path(),
    ) {
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
    CommandOutput::rendered(
        "schema promote",
        render::schema_promote_document(&report),
        report,
    )
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
        let inspection_root = inspection_artifact_root("schema-promote-execute")?;
        let fresh_discovery = match discover_artifacts_for_cli_at(
            context,
            resource,
            execution,
            inspection_root.path(),
        ) {
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
    let settlement_store = SqlitePromotionSettlementStore::open_with_path_ownership(
        &state_path,
        context.state_store_path_ownership(),
    )?;
    let result = execute_schema_promotion(SchemaPromotionExecutionRequest {
        project_root: &context.root,
        package_root: &context.package_root(),
        resource,
        lock: &lock,
        lock_authority: &authority,
        dry_plan: &report,
        destinations,
        execution_services: execution.clone(),
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
        render::schema_promotion_execution_document(&result),
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
    let inspection_root = inspection_artifact_root("schema-discover")?;
    let artifacts =
        discover_artifacts_for_cli_at(&context, resource, execution, inspection_root.path())?;
    let discovery = &artifacts.discovery;
    let report = SchemaDiscoverReport::from_discovery(
        &context,
        &args.resource_id,
        &discovery.snapshot.artifact,
        &discovery.snapshot.source_identity,
        artifacts.discovery_manifest.as_ref(),
    );
    CommandOutput::rendered(
        "schema discover",
        render::schema_discover_document(&report),
        report,
    )
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
    CommandOutput::rendered("schema pin", render::schema_pin_document(&report), report)
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
    CommandOutput::rendered("schema show", render::schema_show_document(&report), report)
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
    let inspection_root = inspection_artifact_root("schema-diff")?;
    let artifacts =
        discover_artifacts_for_cli_at(&context, resource, execution, inspection_root.path())?;
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
    CommandOutput::rendered("schema diff", render::schema_diff_document(&report), report)
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
    let artifact_root = context.root.as_path();
    discover_artifacts_for_cli_at(context, resource, execution, artifact_root)
}

fn discover_artifacts_for_cli_at(
    context: &ProjectContext,
    resource: &CompiledResource,
    execution: &cdf_runtime::ExecutionServices,
    artifact_root: &std::path::Path,
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
    let written = context
        .lock_authority
        .as_ref()
        .map(|authority| authority.bytes.as_slice())
        != Some(encoded.as_bytes());
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
