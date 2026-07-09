use std::{collections::BTreeMap, fs, sync::Arc};

use cdf_declarative::{CompiledResource, CompiledResourcePlan};
use cdf_kernel::{CdfError, ResourceStream, SchemaSnapshotReference, SchemaSource};
use cdf_project::{
    LOCK_FILE_NAME, SchemaSnapshotArtifact, SchemaSnapshotDataType, SchemaSnapshotField,
    SchemaSnapshotStore, discover_resource_schema, lock_to_toml,
    pin_schema_snapshot_in_project_lockfile,
};
use serde::Serialize;

use crate::{
    args::{Cli, SchemaCommand, SchemaDiscoverArgs, SchemaResourceArgs},
    context::ProjectContext,
    http_transport::ReqwestHttpTransport,
    output::{CliError, CommandOutput},
    project_run_resource::file_runtime_dependencies,
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
    },
};

pub(crate) fn schema(cli: &Cli, command: SchemaCommand) -> Result<CommandOutput, CliError> {
    match command {
        SchemaCommand::Discover(args) => discover(cli, args),
        SchemaCommand::Pin(args) => pin(cli, args),
        SchemaCommand::Show(args) => show(cli, args),
        SchemaCommand::Diff(args) => diff(cli, args),
    }
}

fn discover(cli: &Cli, args: SchemaDiscoverArgs) -> Result<CommandOutput, CliError> {
    let context = load_context(cli, "schema discover")?;
    let resource = context.resource(&args.resource_id)?;
    let discovery = discover_for_cli(&context, resource)?;
    let report = SchemaDiscoverReport::from_discovery(
        &context,
        &args.resource_id,
        &discovery.snapshot.artifact,
        &discovery.snapshot.source_identity,
    );
    CommandOutput::rendered("schema discover", schema_discover_document(&report), report)
}

fn pin(cli: &Cli, args: SchemaResourceArgs) -> Result<CommandOutput, CliError> {
    let context = load_context(cli, "schema pin")?;
    let resource = context.resource(&args.resource_id)?;
    let previous = pinned_snapshot_reference(&context, resource).cloned();
    let discovery = discover_for_cli(&context, resource)?;
    let store = SchemaSnapshotStore::new(&context.root);
    let snapshot_written = store.write_if_changed(&discovery.snapshot.artifact)?;
    let pinned_resource = resource.with_schema_source_and_schema(
        SchemaSource::Discovered {
            snapshot: discovery.snapshot.reference.clone(),
        },
        Arc::clone(&discovery.normalized_schema),
    );
    let lockfile = update_lockfile(&context, &pinned_resource)?;
    let status = match previous {
        Some(previous) if previous.schema_hash == discovery.snapshot.artifact.schema_hash => {
            "unchanged"
        }
        Some(_) => "refreshed",
        None => "added",
    };
    let report = SchemaPinReport::from_pin(
        &context,
        &args.resource_id,
        status,
        &discovery.snapshot.artifact,
        &discovery.snapshot.source_identity,
        snapshot_written,
        lockfile,
    );
    CommandOutput::rendered("schema pin", schema_pin_document(&report), report)
}

fn show(cli: &Cli, args: SchemaResourceArgs) -> Result<CommandOutput, CliError> {
    let context = load_context(cli, "schema show")?;
    let resource = context.resource(&args.resource_id)?;
    let reference = pinned_snapshot_reference(&context, resource)
        .ok_or_else(|| no_pinned_snapshot_error(&args.resource_id))?;
    let artifact = SchemaSnapshotStore::new(&context.root).read(reference)?;
    let report = SchemaShowReport::from_artifact(&context, &args.resource_id, &artifact);
    CommandOutput::rendered("schema show", schema_show_document(&report), report)
}

fn diff(cli: &Cli, args: SchemaResourceArgs) -> Result<CommandOutput, CliError> {
    let context = load_context(cli, "schema diff")?;
    let resource = context.resource(&args.resource_id)?;
    let reference = pinned_snapshot_reference(&context, resource)
        .ok_or_else(|| no_pinned_snapshot_error(&args.resource_id))?;
    let pinned = SchemaSnapshotStore::new(&context.root).read(reference)?;
    let fresh = discover_for_cli(&context, resource)?;
    let report = SchemaDiffReport::from_snapshots(
        &context,
        &args.resource_id,
        &pinned,
        &fresh.snapshot.artifact,
    );
    CommandOutput::rendered("schema diff", schema_diff_document(&report), report)
}

fn load_context(cli: &Cli, command: &str) -> Result<ProjectContext, CliError> {
    ProjectContext::load_for_command(command, cli.project.as_ref(), cli.env.as_deref())
}

fn discover_for_cli(
    context: &ProjectContext,
    resource: &CompiledResource,
) -> Result<cdf_project::ResourceSchemaDiscovery, CliError> {
    if matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
        return discover_for_cli_resource(context, resource);
    }
    if resource
        .descriptor()
        .schema_source
        .pinned_snapshot()
        .is_some()
    {
        let probe_resource =
            resource.with_schema_source_and_schema(SchemaSource::Discover, resource.schema());
        return discover_for_cli_resource(context, &probe_resource);
    }
    discover_for_cli_resource(context, resource)
}

fn discover_for_cli_resource(
    context: &ProjectContext,
    resource: &CompiledResource,
) -> Result<cdf_project::ResourceSchemaDiscovery, CliError> {
    let secret_provider = context.secret_provider();
    if matches!(resource.descriptor().schema_source, SchemaSource::Discover)
        && matches!(resource.plan(), CompiledResourcePlan::Files(plan) if is_http_file_plan(plan))
    {
        return Ok(
            cdf_project::discover_resource_schema_with_file_dependencies(
                resource,
                &secret_provider,
                file_runtime_dependencies(context)?,
            )?,
        );
    }
    if matches!(resource.descriptor().schema_source, SchemaSource::Discover)
        && matches!(resource.plan(), CompiledResourcePlan::Rest(_))
    {
        let mut transport = ReqwestHttpTransport::new()?;
        Ok(cdf_project::discover_resource_schema_with_rest_transport(
            resource,
            &secret_provider,
            &mut transport,
        )?)
    } else {
        Ok(discover_resource_schema(resource, &secret_provider)?)
    }
}

fn is_http_file_plan(plan: &cdf_declarative::FileResourcePlan) -> bool {
    plan.root.starts_with("http://") || plan.root.starts_with("https://")
}

fn update_lockfile(
    context: &ProjectContext,
    pinned_resource: &CompiledResource,
) -> Result<SchemaLockfileWrite, CliError> {
    let updated = pin_schema_snapshot_in_project_lockfile(
        &context.config,
        &context.resources,
        context.lock.as_ref(),
        &context.environment.destination,
        pinned_resource,
    )?;
    let encoded = lock_to_toml(&updated)?;
    let path = context.root.join(LOCK_FILE_NAME);
    let written = fs::read_to_string(&path).ok().as_deref() != Some(&encoded);
    if written {
        fs::write(&path, encoded).map_err(|error| {
            CliError::from(CdfError::data(format!("write {}: {error}", path.display())))
        })?;
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
    writes: SchemaWrites,
    next_command: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaPinReport {
    #[serde(flatten)]
    snapshot: SchemaSnapshotReportBase,
    status: String,
    source_identity: BTreeMap<String, String>,
    writes: SchemaWrites,
    unsupported: Vec<String>,
    next_command: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaShowReport {
    #[serde(flatten)]
    snapshot: SchemaSnapshotReportBase,
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
    ) -> Self {
        Self {
            snapshot: SchemaSnapshotReportBase::from_artifact(context, resource_id, artifact),
            source_identity: source_identity.clone(),
            writes: SchemaWrites::none(),
            next_command: format!("cdf plan {resource_id}"),
        }
    }
}

impl SchemaPinReport {
    fn from_pin(
        context: &ProjectContext,
        resource_id: &str,
        status: &str,
        artifact: &SchemaSnapshotArtifact,
        source_identity: &BTreeMap<String, String>,
        snapshot_written: bool,
        lockfile: SchemaLockfileWrite,
    ) -> Self {
        let unsupported = lockfile.unsupported_reason.into_iter().collect::<Vec<_>>();
        Self {
            snapshot: SchemaSnapshotReportBase::from_artifact(context, resource_id, artifact),
            status: status.to_owned(),
            source_identity: source_identity.clone(),
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
    ) -> Self {
        Self {
            snapshot: SchemaSnapshotReportBase::from_artifact(context, resource_id, artifact),
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
            unsupported: &[],
            next_command: Some(&report.next_command),
        },
    )
}

struct SnapshotDocumentData<'a> {
    base: &'a SchemaSnapshotReportBase,
    writes: &'a SchemaWrites,
    source_identity: Option<&'a BTreeMap<String, String>>,
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
    RenderDocument::new()
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
        .push(diff_table(report))
        .blank_line()
        .push(writes_panel(&report.writes))
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
