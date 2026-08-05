mod render;

use std::{collections::BTreeMap, path::PathBuf};

use cdf_project::{EffectiveEnvironment, LockedDestination, ProjectConfig};
use serde::{Serialize, de::DeserializeOwned};

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
            let context = ProjectContext::load_for_command_with_destination_registry(
                inspect_command_name(&noun),
                cli.project.as_ref(),
                cli.env.as_deref(),
                true,
                destinations,
            )?;
            match noun {
                InspectNoun::Project => {
                    let resource_count = context.resources.len();
                    let report = InspectProjectReport {
                        root: context.root,
                        config: redact_typed(context.config)?,
                        environment: redact_typed(context.environment)?,
                        resource_count,
                    };
                    CommandOutput::rendered(
                        "inspect project",
                        render::project_document(&report),
                        report,
                    )
                }
                InspectNoun::Resources => {
                    let report = InspectResourcesReport(resource_summaries(&context)?);
                    CommandOutput::rendered(
                        "inspect resources",
                        render::resources_document(&report),
                        report,
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
                    let report = InspectLockReport(redact_typed(require_lock(&context)?.clone())?);
                    CommandOutput::rendered("inspect lock", render::lock_document(&report), report)
                }
                InspectNoun::Destinations => {
                    let runtime =
                        redact_destination_runtime(context.destination_runtime(destinations));
                    let report = InspectDestinationsReport {
                        environment_destination: redact_uri_userinfo(
                            &context.environment.destination,
                        ),
                        runtime,
                        locked: redact_typed(context.lock.map(|lock| lock.destinations))?,
                    };
                    CommandOutput::rendered(
                        "inspect destinations",
                        render::destinations_document(&report),
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
    let manifest = redact_typed(cdf_package::read_manifest(&path)?)?;
    let report = InspectPackageReport { path, manifest };
    CommandOutput::rendered("inspect package", render::package_document(&report), report)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct InspectProjectReport {
    root: PathBuf,
    config: ProjectConfig,
    environment: EffectiveEnvironment,
    #[serde(skip)]
    resource_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
struct InspectResourcesReport(Vec<ResourceSummary>);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
struct InspectLockReport(cdf_project::CdfLock);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct InspectDestinationsReport {
    environment_destination: String,
    runtime: crate::context::DestinationRuntime,
    locked: Option<BTreeMap<String, LockedDestination>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct InspectPackageReport {
    #[serde(skip)]
    path: PathBuf,
    #[serde(flatten)]
    manifest: cdf_package_contract::PackageManifest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ResourceSummary {
    descriptor: cdf_kernel::ResourceDescriptor,
    configured_source: String,
    namespace: String,
    resource_name: String,
    resource_file: String,
    target: String,
    capabilities: cdf_kernel::ResourceCapabilities,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_capabilities: Option<cdf_runtime::SourceStreamCapabilities>,
}

impl ResourceSummary {
    fn from_compiled(
        resource: &cdf_declarative::CompiledResource,
        query: &cdf_project::ProjectQueryCompilation,
    ) -> Self {
        Self {
            descriptor: resource.descriptor().clone(),
            configured_source: query.configured_source.configured_source.clone(),
            namespace: query.namespace.clone(),
            resource_name: query.resource_name.clone(),
            resource_file: query.relative_path.clone(),
            target: query.effective.target.value.to_string(),
            capabilities: resource.capabilities().clone(),
            stream_capabilities: resource.source_plan().stream_capabilities.clone(),
        }
    }
}

fn redact_typed<T>(value: T) -> Result<T, CliError>
where
    T: Serialize + DeserializeOwned,
{
    let value = serde_json::to_value(value).map_err(crate::commands::json_cli_error)?;
    serde_json::from_value(redact_json_uri_userinfo(value)).map_err(crate::commands::json_cli_error)
}

fn redact_destination_runtime(
    mut runtime: crate::context::DestinationRuntime,
) -> crate::context::DestinationRuntime {
    runtime.label = runtime.label.map(|value| redact_uri_userinfo(&value));
    runtime.error = runtime.error.map(|value| redact_uri_userinfo(&value));
    for health in &mut runtime.health {
        health.message = redact_uri_userinfo(&health.message);
        health.details = std::mem::take(&mut health.details)
            .into_iter()
            .map(|(key, value)| (key, redact_json_uri_userinfo(value)))
            .collect();
    }
    runtime
}

fn redact_json_uri_userinfo(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(value) => serde_json::Value::String(redact_uri_userinfo(&value)),
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(redact_json_uri_userinfo).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, redact_json_uri_userinfo(value)))
                .collect(),
        ),
        value => value,
    }
}

fn resource_summary(context: &ProjectContext, id: &str) -> Result<ResourceSummary, CliError> {
    let resource = context.resource(id)?;
    let query = context
        .resource_query(id)
        .ok_or_else(|| cdf_kernel::CdfError::internal("resource lost its query compilation"))?;
    Ok(ResourceSummary::from_compiled(resource, query))
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
