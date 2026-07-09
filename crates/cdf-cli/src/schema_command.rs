use std::collections::BTreeMap;

use cdf_declarative::CompiledResourcePlan;
use cdf_kernel::SchemaSource;
use cdf_project::{SchemaSnapshotDataType, discover_resource_schema};
use serde::Serialize;

use crate::{
    args::{Cli, SchemaCommand, SchemaDiscoverArgs},
    context::ProjectContext,
    http_transport::ReqwestHttpTransport,
    output::{CliError, CommandOutput},
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
    },
};

pub(crate) fn schema(cli: &Cli, command: SchemaCommand) -> Result<CommandOutput, CliError> {
    match command {
        SchemaCommand::Discover(args) => discover(cli, args),
    }
}

fn discover(cli: &Cli, args: SchemaDiscoverArgs) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load_for_command(
        "schema discover",
        cli.project.as_ref(),
        cli.env.as_deref(),
    )?;
    let resource = context.resource(&args.resource_id)?;
    let secret_provider = context.secret_provider();
    let discovery = if matches!(resource.descriptor().schema_source, SchemaSource::Discover)
        && matches!(resource.plan(), CompiledResourcePlan::Rest(_))
    {
        let mut transport = ReqwestHttpTransport::new()?;
        cdf_project::discover_resource_schema_with_rest_transport(
            resource,
            &secret_provider,
            &mut transport,
        )?
    } else {
        discover_resource_schema(resource, &secret_provider)?
    };
    let report = SchemaDiscoverReport::from_discovery(
        &context,
        &args.resource_id,
        &discovery.snapshot.artifact,
        &discovery.snapshot.source_identity,
    );
    CommandOutput::rendered("schema discover", schema_discover_document(&report), report)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaDiscoverReport {
    project: String,
    environment: String,
    resource_id: String,
    schema_hash: String,
    schema_snapshot_path: String,
    snapshot_metadata: BTreeMap<String, String>,
    fields: Vec<SchemaDiscoverFieldReport>,
    source_identity: BTreeMap<String, String>,
    writes: SchemaDiscoverWrites,
    next_command: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaDiscoverFieldReport {
    name: String,
    data_type: SchemaSnapshotDataType,
    nullable: bool,
    source_name: Option<String>,
    metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SchemaDiscoverWrites {
    schema_snapshot: bool,
    lockfile: bool,
    package: bool,
    destination: bool,
    checkpoint: bool,
}

impl SchemaDiscoverReport {
    fn from_discovery(
        context: &ProjectContext,
        resource_id: &str,
        artifact: &cdf_project::SchemaSnapshotArtifact,
        source_identity: &BTreeMap<String, String>,
    ) -> Self {
        Self {
            project: context.config.project.name.clone(),
            environment: context.environment.name.clone(),
            resource_id: resource_id.to_owned(),
            schema_hash: artifact.schema_hash.to_string(),
            schema_snapshot_path: artifact.path.clone(),
            snapshot_metadata: artifact.metadata.clone(),
            fields: artifact
                .schema
                .fields
                .iter()
                .map(|field| SchemaDiscoverFieldReport {
                    name: field.name.clone(),
                    data_type: field.data_type.clone(),
                    nullable: field.nullable,
                    source_name: field.metadata.get("cdf:source_name").cloned(),
                    metadata: field.metadata.clone(),
                })
                .collect(),
            source_identity: source_identity.clone(),
            writes: SchemaDiscoverWrites {
                schema_snapshot: false,
                lockfile: false,
                package: false,
                destination: false,
                checkpoint: false,
            },
            next_command: format!("cdf plan {resource_id}"),
        }
    }
}

fn schema_discover_document(report: &SchemaDiscoverReport) -> RenderDocument {
    let mut source_identity = KeyValuePanel::new("Source Identity");
    for (key, value) in &report.source_identity {
        source_identity = source_identity.row(key.clone(), value.clone());
    }

    RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            format!("discovered schema for {}", report.resource_id),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Schema")
                .row("project", report.project.clone())
                .row("environment", report.environment.clone())
                .row("resource", report.resource_id.clone())
                .row("hash", report.schema_hash.clone())
                .row("candidate path", report.schema_snapshot_path.clone())
                .row(
                    "probe",
                    report
                        .snapshot_metadata
                        .get("probe")
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_owned()),
                )
                .row(
                    "normalizer",
                    report
                        .snapshot_metadata
                        .get("cdf:normalizer")
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_owned()),
                ),
        )
        .blank_line()
        .push(field_table(&report.fields))
        .blank_line()
        .push(source_identity)
        .blank_line()
        .push(
            KeyValuePanel::new("Writes")
                .row("schema snapshot", yes_no(report.writes.schema_snapshot))
                .row("lockfile", yes_no(report.writes.lockfile))
                .row("package", yes_no(report.writes.package))
                .row("destination", yes_no(report.writes.destination))
                .row("checkpoint", yes_no(report.writes.checkpoint)),
        )
        .blank_line()
        .push(NextCommand::new(report.next_command.clone()))
}

fn field_table(fields: &[SchemaDiscoverFieldReport]) -> Table {
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

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
