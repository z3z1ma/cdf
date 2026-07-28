mod render;

use serde::Serialize;
use serde_json::json;
use std::path::PathBuf;

use crate::{
    args::{Cli, InspectArgs, InspectNoun},
    context::{ProjectContext, require_lock},
    output::{CliError, CommandOutput},
    render::redaction::redact_uri_userinfo,
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
                        render::project_document(&context),
                        report,
                    )
                }
                InspectNoun::Resources => {
                    let resources = resource_summaries(&context)?;
                    CommandOutput::rendered(
                        "inspect resources",
                        render::resources_document(&resources),
                        resources,
                    )
                }
                InspectNoun::Resource(id) => {
                    let report = resource_summary(&context, &id)?;
                    CommandOutput::rendered(
                        "inspect resource",
                        render::resource_document(&report),
                        report,
                    )
                }
                InspectNoun::Lock => {
                    let lock = require_lock(&context)?;
                    CommandOutput::rendered("inspect lock", render::lock_document(lock), lock)
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
                        render::destinations_document(&context, &runtime),
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
        render::package_document(&path, &manifest),
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
