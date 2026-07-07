use std::path::PathBuf;

use cdf_package::PackageReader;
use serde::Serialize;
use serde_json::json;

use crate::{
    args::{Cli, InspectArgs, InspectNoun},
    commands::output,
    context::{ProjectContext, require_lock},
    output::{CliError, CommandOutput},
};

pub(crate) fn inspect(cli: &Cli, args: InspectArgs) -> Result<CommandOutput, CliError> {
    match args.noun {
        InspectNoun::Package(path) => inspect_package(path),
        noun => {
            let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
            match noun {
                InspectNoun::Project => output(
                    "inspect project",
                    format!(
                        "project {} env {}",
                        context.config.project.name, context.environment.name
                    ),
                    json!({
                        "root": context.root,
                        "config": context.config,
                        "environment": context.environment,
                    }),
                ),
                InspectNoun::Resources => {
                    let resources = resource_summaries(&context);
                    output(
                        "inspect resources",
                        format!("{} compiled resource(s)", resources.len()),
                        resources,
                    )
                }
                InspectNoun::Resource(id) => {
                    let resource = context.resource(&id)?;
                    output(
                        "inspect resource",
                        format!("resource {id}"),
                        ResourceSummary::from_resource(resource),
                    )
                }
                InspectNoun::Lock => {
                    let lock = require_lock(&context)?;
                    output(
                        "inspect lock",
                        format!(
                            "lockfile v{} for project {}",
                            lock.version, lock.project.name
                        ),
                        lock,
                    )
                }
                InspectNoun::Destinations => {
                    let runtime = context.destination_runtime();
                    output(
                        "inspect destinations",
                        "inspected destination capabilities".to_owned(),
                        json!({
                            "environment_destination": context.environment.destination,
                            "runtime": runtime,
                            "locked": context.lock.as_ref().map(|lock| &lock.destinations),
                        }),
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
    output(
        "inspect package",
        format!(
            "package {} status {}",
            reader.manifest().package_hash,
            reader.manifest().lifecycle.status.as_str()
        ),
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
