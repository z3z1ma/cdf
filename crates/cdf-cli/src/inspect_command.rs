mod render;

use std::{collections::BTreeMap, path::PathBuf};

use cdf_project::{EffectiveEnvironment, LockedDestination, ProjectConfig};
use serde::{Serialize, Serializer};

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
                    let report = InspectProjectReport {
                        root: context.root,
                        config: context.config,
                        environment: context.environment,
                        resource_count: context.resources.len(),
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
                    let report = InspectLockReport(require_lock(&context)?.clone());
                    CommandOutput::rendered("inspect lock", render::lock_document(&report), report)
                }
                InspectNoun::Destinations => {
                    let runtime = context.destination_runtime(destinations);
                    let report = InspectDestinationsReport {
                        environment_destination: redact_uri_userinfo(
                            &context.environment.destination,
                        ),
                        runtime,
                        locked: context.lock.map(|lock| lock.destinations),
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
    let manifest = cdf_package::read_manifest(&path)?;
    let report = InspectPackageReport { path, manifest };
    CommandOutput::rendered("inspect package", render::package_document(&report), report)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct InspectProjectReport {
    root: PathBuf,
    #[serde(serialize_with = "serialize_redacted")]
    config: ProjectConfig,
    #[serde(serialize_with = "serialize_redacted")]
    environment: EffectiveEnvironment,
    #[serde(skip)]
    resource_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
struct InspectResourcesReport(Vec<ResourceSummary>);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
struct InspectLockReport(#[serde(serialize_with = "serialize_redacted")] cdf_project::CdfLock);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct InspectDestinationsReport {
    environment_destination: String,
    #[serde(serialize_with = "serialize_redacted")]
    runtime: crate::context::DestinationRuntime,
    #[serde(serialize_with = "serialize_redacted")]
    locked: Option<BTreeMap<String, LockedDestination>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct InspectPackageReport {
    #[serde(skip)]
    path: PathBuf,
    #[serde(flatten, serialize_with = "serialize_redacted")]
    manifest: cdf_package_contract::PackageManifest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ResourceSummary {
    #[serde(serialize_with = "serialize_redacted")]
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
            source_file: source_file.map(|value| redact_uri_userinfo(&value)),
            mapping_pattern: mapping_pattern.map(|value| redact_uri_userinfo(&value)),
            mapping_status: mapping_status.map(|value| redact_uri_userinfo(&value)),
            capabilities: resource.capabilities().clone(),
            stream_capabilities: resource.source_plan().stream_capabilities.clone(),
        }
    }
}

fn serialize_redacted<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: Serialize,
    S: Serializer,
{
    let value = serde_json::to_value(value).map_err(serde::ser::Error::custom)?;
    redact_json_uri_userinfo(value).serialize(serializer)
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
