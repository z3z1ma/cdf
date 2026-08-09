mod render;

use std::{
    fs, io,
    path::{Component, Path, PathBuf},
};

use cdf_kernel::CdfError;
use cdf_project::{EffectiveEnvironment, ProjectConfig, resolve_project_resource_selection};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    args::{Cli, InspectArgs, InspectNoun},
    context::{ProjectContext, ProjectOperationalContext},
    output::{CliError, CommandOutput},
    render::redaction::redact_uri_userinfo,
};

pub(crate) fn inspect(
    cli: &Cli,
    args: InspectArgs,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    match args.noun {
        InspectNoun::Package(path) => inspect_package(cli, path),
        InspectNoun::Run(id) => {
            let context =
                ProjectOperationalContext::load(cli.project.as_ref(), cli.env.as_deref())?;
            crate::inspect_run_command::inspect_run(&context, id)
        }
        InspectNoun::Project => {
            let context =
                ProjectOperationalContext::load(cli.project.as_ref(), cli.env.as_deref())?;
            let resource_count = resolve_project_resource_selection(&context.root, &[], &[])
                .map_err(|error| match error {
                    cdf_project::ProjectResourceSelectionError::Project(error) => error,
                    error => cdf_kernel::CdfError::contract(error.to_string()),
                })?
                .resources
                .len();
            let report = InspectProjectReport {
                root: context.root,
                config: redact_typed(context.config)?,
                environment: redact_typed(context.environment)?,
                resource_count,
            };
            CommandOutput::rendered("inspect project", render::project_document(&report), report)
        }
        InspectNoun::Resource(id) => {
            let operational =
                ProjectOperationalContext::load(cli.project.as_ref(), cli.env.as_deref())?;
            resolve_project_resource_selection(&operational.root, std::slice::from_ref(&id), &[])
                .map_err(|error| {
                crate::compile_command::resource_selection_error("cdf inspect resource", error)
            })?;
            let context = ProjectContext::load_selected_read_only(
                cli.project.as_ref(),
                cli.env.as_deref(),
                &id,
                destinations,
            )?;
            let report = resource_summary(&context, &id)?;
            CommandOutput::rendered(
                "inspect resource",
                render::resource_document(&report),
                report,
            )
        }
        InspectNoun::Destinations => {
            let operational =
                ProjectOperationalContext::load(cli.project.as_ref(), cli.env.as_deref())?;
            let runtime = redact_destination_runtime(operational.destination_runtime(destinations));
            let report = InspectDestinationsReport {
                environment_destination: redact_uri_userinfo(&operational.environment.destination),
                runtime,
            };
            CommandOutput::rendered(
                "inspect destinations",
                render::destinations_document(&report),
                report,
            )
        }
        InspectNoun::Resources => {
            let context = ProjectContext::load_for_command_with_destination_registry(
                cli.project.as_ref(),
                cli.env.as_deref(),
                destinations,
            )?;
            let report = InspectResourcesReport(resource_summaries(&context)?);
            CommandOutput::rendered(
                "inspect resources",
                render::resources_document(&report),
                report,
            )
        }
    }
}

fn inspect_package(cli: &Cli, operand: PathBuf) -> Result<CommandOutput, CliError> {
    let (path, expected_id) = match package_identifier(&operand) {
        Some(package_id) => {
            let context =
                ProjectOperationalContext::load(cli.project.as_ref(), cli.env.as_deref())?;
            let package_root = context.package_root();
            let path = package_root.join(package_id);
            match fs::symlink_metadata(&path) {
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    return Err(CdfError::data(format!(
                        "package identifier `{package_id}` was not found under {}; run `cdf package ls` for the selected project and environment",
                        package_root.display()
                    ))
                    .into());
                }
                Err(error) => {
                    return Err(CdfError::environment(format!(
                        "inspect package identifier `{package_id}` under {}: {error}; check path permissions and device health before retrying",
                        package_root.display()
                    ))
                    .into());
                }
            }
            (path, Some(package_id))
        }
        None => (operand, None),
    };
    let manifest = redact_typed(cdf_package::read_manifest(&path)?)?;
    if let Some(expected_id) = expected_id
        && manifest.identity.package_id != expected_id
    {
        return Err(CdfError::data(format!(
            "package identifier `{expected_id}` resolved to a manifest declaring `{}`; inspect the package root and retry with an exact package id",
            manifest.identity.package_id
        ))
        .into());
    }
    let report = InspectPackageReport { path, manifest };
    CommandOutput::rendered("inspect package", render::package_document(&report), report)
}

fn package_identifier(operand: &Path) -> Option<&str> {
    if operand.is_absolute() {
        return None;
    }
    let mut components = operand.components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(identifier)), None) => identifier.to_str(),
        _ => None,
    }
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
struct InspectDestinationsReport {
    environment_destination: String,
    runtime: crate::context::DestinationRuntime,
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
