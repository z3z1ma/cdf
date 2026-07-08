use std::path::{Path, PathBuf};

use cdf_package::PackageReader;
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

pub(crate) fn inspect(cli: &Cli, args: InspectArgs) -> Result<CommandOutput, CliError> {
    match args.noun {
        InspectNoun::Package(path) => inspect_package(path),
        noun => {
            let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
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
                    let resources = resource_summaries(&context);
                    CommandOutput::rendered(
                        "inspect resources",
                        inspect_resources_document(&resources),
                        resources,
                    )
                }
                InspectNoun::Resource(id) => {
                    let resource = context.resource(&id)?;
                    let report = ResourceSummary::from_resource(resource);
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
                    let runtime = context.destination_runtime();
                    let report = json!({
                            "environment_destination": context.environment.destination,
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

fn inspect_package(path: PathBuf) -> Result<CommandOutput, CliError> {
    let reader = PackageReader::open(&path)?;
    CommandOutput::rendered(
        "inspect package",
        inspect_package_document(&path, reader.manifest()),
        reader.manifest(),
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ResourceSummary {
    descriptor: cdf_kernel::ResourceDescriptor,
    capabilities: cdf_kernel::ResourceCapabilities,
}

impl ResourceSummary {
    fn from_resource(resource: &cdf_declarative::CompiledResource) -> Self {
        Self {
            descriptor: resource.descriptor().clone(),
            capabilities: resource.capabilities().clone(),
        }
    }
}

fn resource_summaries(context: &ProjectContext) -> Vec<ResourceSummary> {
    context
        .resources
        .iter()
        .map(ResourceSummary::from_resource)
        .collect()
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
        Table::new(["resource", "trust", "cursor"]),
        |table, resource| {
            table.row([
                resource.descriptor.resource_id.to_string(),
                format!("{:?}", resource.descriptor.trust_level).to_lowercase(),
                resource
                    .descriptor
                    .cursor
                    .as_ref()
                    .map(|cursor| cursor.field.clone())
                    .unwrap_or_else(|| "none".to_owned()),
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
                .row("capabilities", format!("{:?}", resource.capabilities)),
        )
        .blank_line()
        .push(NextCommand::new(format!(
            "cdf plan {}",
            resource.descriptor.resource_id
        )))
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
    RenderDocument::new()
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
        )
        .blank_line()
        .push(NextCommand::new("cdf plan"))
}

fn inspect_package_document(
    path: &Path,
    manifest: &cdf_package::PackageManifest,
) -> RenderDocument {
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
