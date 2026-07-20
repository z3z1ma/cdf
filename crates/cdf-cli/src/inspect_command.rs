use std::path::{Path, PathBuf};

use cdf_package_contract::PackageManifest;
use serde::Serialize;
use serde_json::json;

use crate::{
    args::{Cli, InspectArgs, InspectNoun},
    context::{DestinationRuntime, ProjectContext, require_lock},
    output::{CliError, CommandOutput},
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
        redaction::redact_uri_userinfo,
    },
};

pub(crate) fn inspect(
    cli: &Cli,
    args: InspectArgs,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    match args.noun {
        InspectNoun::Package(path) => inspect_package(path),
        noun => {
            let context = ProjectContext::load_for_command(
                inspect_command_name(&noun),
                cli.project.as_ref(),
                cli.env.as_deref(),
            )?;
            match noun {
                InspectNoun::Project => {
                    let report = json!({
                        "root": context.root,
                        "config": context.config,
                        "environment": context.environment,
                    });
                    CommandOutput::rendered(
                        "inspect project",
                        inspect_project_document(&context),
                        report,
                    )
                }
                InspectNoun::Resources => {
                    let resources = resource_summaries(&context)?;
                    CommandOutput::rendered(
                        "inspect resources",
                        inspect_resources_document(&resources),
                        resources,
                    )
                }
                InspectNoun::Resource(id) => {
                    let report = resource_summary(&context, &id)?;
                    CommandOutput::rendered(
                        "inspect resource",
                        inspect_resource_document(&report),
                        report,
                    )
                }
                InspectNoun::Lock => {
                    let lock = require_lock(&context)?;
                    CommandOutput::rendered("inspect lock", inspect_lock_document(lock), lock)
                }
                InspectNoun::Destinations => {
                    let runtime = context.destination_runtime(destinations);
                    let report = json!({
                            "environment_destination": redact_uri_userinfo(
                                &context.environment.destination
                            ),
                            "runtime": runtime,
                            "locked": context.lock.as_ref().map(|lock| &lock.destinations),
                    });
                    CommandOutput::rendered(
                        "inspect destinations",
                        inspect_destinations_document(&context, &runtime),
                        report,
                    )
                }
                InspectNoun::Run(id) => crate::inspect_run_command::inspect_run(&context, id),
                InspectNoun::Package(_) => unreachable!("package noun handled before project load"),
            }
        }
    }
}

fn inspect_command_name(noun: &InspectNoun) -> &'static str {
    match noun {
        InspectNoun::Project => "inspect project",
        InspectNoun::Resources => "inspect resources",
        InspectNoun::Resource(_) => "inspect resource",
        InspectNoun::Lock => "inspect lock",
        InspectNoun::Destinations => "inspect destinations",
        InspectNoun::Run(_) => "inspect run",
        InspectNoun::Package(_) => "inspect package",
    }
}

fn inspect_package(path: PathBuf) -> Result<CommandOutput, CliError> {
    let manifest = cdf_package::read_manifest(&path)?;
    CommandOutput::rendered(
        "inspect package",
        inspect_package_document(&path, &manifest),
        manifest,
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ResourceSummary {
    descriptor: cdf_kernel::ResourceDescriptor,
    source_name: String,
    resource_name: String,
    source_file: Option<String>,
    mapping_pattern: Option<String>,
    mapping_status: Option<String>,
    capabilities: cdf_kernel::ResourceCapabilities,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_capabilities: Option<cdf_runtime::SourceStreamCapabilities>,
}

impl ResourceSummary {
    fn from_compiled(
        resource: &cdf_declarative::CompiledResource,
        source_name: &str,
        resource_name: &str,
        source_file: Option<String>,
        mapping_pattern: Option<String>,
        mapping_status: Option<String>,
    ) -> Self {
        Self {
            descriptor: resource.descriptor().clone(),
            source_name: source_name.to_owned(),
            resource_name: resource_name.to_owned(),
            source_file,
            mapping_pattern,
            mapping_status,
            capabilities: resource.capabilities().clone(),
            stream_capabilities: resource.source_plan().stream_capabilities.clone(),
        }
    }
}

fn resource_summary(context: &ProjectContext, id: &str) -> Result<ResourceSummary, CliError> {
    let origin = context.resource_origin(id);
    let mapping = context.config.resources.get(id);
    let (default_source, default_resource) = id.split_once('.').unwrap_or((id, id));
    let source_name = origin
        .map(|origin| origin.source_name.clone())
        .unwrap_or_else(|| default_source.to_owned());
    let resource_name = origin
        .map(|origin| origin.resource_name.clone())
        .unwrap_or_else(|| default_resource.to_owned());
    let source_file = origin
        .and_then(|origin| origin.source_file.clone())
        .or_else(|| mapping.map(|mapping| mapping.source.clone()));
    let mapping_pattern = origin
        .map(|origin| origin.mapping_pattern.clone())
        .or_else(|| mapping.map(|_| id.to_owned()));
    let mapping_status = origin
        .map(|origin| origin.mapping_status.clone())
        .or_else(|| mapping.map(|_| "matched".to_owned()));
    if let Some(resource) =
        crate::project_run_resource::build_project_resource_for_inspection(context, id)?
    {
        return Ok(ResourceSummary::from_compiled(
            &resource,
            &source_name,
            &resource_name,
            source_file,
            mapping_pattern,
            mapping_status,
        ));
    }
    let resource = context.resource(id)?;
    Ok(ResourceSummary::from_compiled(
        resource,
        &source_name,
        &resource_name,
        source_file,
        mapping_pattern,
        mapping_status,
    ))
}

fn resource_summaries(context: &ProjectContext) -> Result<Vec<ResourceSummary>, CliError> {
    let mut summaries = context
        .resource_ids()
        .into_iter()
        .map(|id| resource_summary(context, &id))
        .collect::<Result<Vec<_>, CliError>>()?;
    summaries.sort_by(|left, right| {
        left.descriptor
            .resource_id
            .cmp(&right.descriptor.resource_id)
    });
    Ok(summaries)
}

fn inspect_project_document(context: &ProjectContext) -> RenderDocument {
    RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            format!(
                "project {} env {}",
                context.config.project.name, context.environment.name
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Project")
                .row("root", path_display(&context.root))
                .row("name", context.config.project.name.clone())
                .row("environment", context.environment.name.clone())
                .row("resources", context.resources.len().to_string())
                .row(
                    "destination",
                    redact_uri_userinfo(&context.environment.destination),
                ),
        )
        .blank_line()
        .push(NextCommand::new("cdf inspect resources"))
}

fn inspect_resources_document(resources: &[ResourceSummary]) -> RenderDocument {
    let table = resources.iter().fold(
        Table::new([
            "compiled id",
            "source",
            "resource",
            "source file",
            "mapping",
        ]),
        |table, resource| {
            table.row([
                resource.descriptor.resource_id.to_string(),
                resource.source_name.clone(),
                resource.resource_name.clone(),
                resource
                    .source_file
                    .clone()
                    .unwrap_or_else(|| "n/a".to_owned()),
                mapping_display(resource),
            ])
        },
    );

    RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            format!("{} compiled resource(s)", resources.len()),
        ))
        .blank_line()
        .push(table)
        .blank_line()
        .push(NextCommand::new("cdf plan"))
}

fn inspect_resource_document(resource: &ResourceSummary) -> RenderDocument {
    RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            format!("resource {}", resource.descriptor.resource_id),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Resource")
                .row("id", resource.descriptor.resource_id.to_string())
                .row("source", resource.source_name.clone())
                .row("resource", resource.resource_name.clone())
                .row(
                    "source file",
                    resource
                        .source_file
                        .clone()
                        .unwrap_or_else(|| "n/a".to_owned()),
                )
                .row("mapping", mapping_display(resource))
                .row(
                    "trust",
                    format!("{:?}", resource.descriptor.trust_level).to_lowercase(),
                )
                .row(
                    "state scope",
                    state_scope_display(&resource.descriptor.state_scope),
                )
                .row(
                    "cursor",
                    resource
                        .descriptor
                        .cursor
                        .as_ref()
                        .map(|cursor| cursor.field.clone())
                        .unwrap_or_else(|| "none".to_owned()),
                )
                .row("capabilities", format!("{:?}", resource.capabilities))
                .row(
                    "stream capabilities",
                    resource
                        .stream_capabilities
                        .as_ref()
                        .map_or_else(|| "bounded".to_owned(), |value| format!("{value:?}")),
                ),
        )
        .blank_line()
        .push(NextCommand::new(format!(
            "cdf plan {}",
            resource.descriptor.resource_id
        )))
}

fn mapping_display(resource: &ResourceSummary) -> String {
    match (&resource.mapping_status, &resource.mapping_pattern) {
        (Some(status), Some(pattern)) => format!("{status} {pattern}"),
        (Some(status), None) => status.clone(),
        (None, Some(pattern)) => pattern.clone(),
        (None, None) => "n/a".to_owned(),
    }
}

fn inspect_lock_document(lock: &cdf_project::CdfLock) -> RenderDocument {
    RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            format!(
                "lockfile v{} for project {}",
                lock.version, lock.project.name
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Lock")
                .row("version", lock.version.to_string())
                .row("project", lock.project.name.clone())
                .row("default env", lock.project.default_environment.clone())
                .row("resources", lock.resources.len().to_string())
                .row("destinations", lock.destinations.len().to_string()),
        )
        .blank_line()
        .push(NextCommand::new("cdf validate"))
}

fn inspect_destinations_document(
    context: &ProjectContext,
    runtime: &DestinationRuntime,
) -> RenderDocument {
    let mut document = RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            "inspected destination capabilities",
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Destination")
                .row(
                    "environment",
                    redact_uri_userinfo(&context.environment.destination),
                )
                .row(
                    "runtime",
                    serde_json::to_value(runtime)
                        .ok()
                        .and_then(|value| {
                            value
                                .get("kind")
                                .and_then(|kind| kind.as_str())
                                .map(str::to_owned)
                        })
                        .unwrap_or_else(|| "unknown".to_owned()),
                )
                .row(
                    "locked",
                    context
                        .lock
                        .as_ref()
                        .map(|lock| lock.destinations.len().to_string())
                        .unwrap_or_else(|| "none".to_owned()),
                ),
        );
    if let Some(capabilities) = &runtime.capabilities {
        let selected = capabilities.bulk_path.as_deref();
        let paths = capabilities.bulk_paths.iter().fold(
            Table::new(["path", "version", "selection", "fallback", "evidence"]),
            |table, path| {
                table.row([
                    path.path_id.clone(),
                    path.version.to_string(),
                    if selected == Some(path.path_id.as_str()) {
                        "selected".to_owned()
                    } else {
                        "available".to_owned()
                    },
                    path.fallback.to_string(),
                    path.measured_evidence_version
                        .clone()
                        .unwrap_or_else(|| "unmeasured".to_owned()),
                ])
            },
        );
        document = document.blank_line().push(paths);
    }
    document.blank_line().push(NextCommand::new("cdf plan"))
}

fn inspect_package_document(path: &Path, manifest: &PackageManifest) -> RenderDocument {
    RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            format!(
                "package {} status {}",
                manifest.package_hash,
                manifest.lifecycle.status.as_str()
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Package")
                .row("path", path_display(path))
                .row("package", manifest.identity.package_id.to_string())
                .row("hash", manifest.package_hash.to_string())
                .row("status", manifest.lifecycle.status.as_str().to_owned())
                .row("files", manifest.identity.files.len().to_string())
                .row("segments", manifest.identity.segments.len().to_string()),
        )
        .blank_line()
        .push(NextCommand::new("cdf package verify"))
}

fn path_display(path: &Path) -> String {
    redact_uri_userinfo(path.display().to_string())
}

fn state_scope_display(scope: &cdf_kernel::ScopeKey) -> String {
    serde_json::to_string(scope).unwrap_or_else(|_| format!("{scope:?}"))
}
