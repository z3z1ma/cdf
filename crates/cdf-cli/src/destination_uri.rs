use cdf_kernel::{CdfError, TargetName};
use cdf_project::{
    ProjectResolutionContext, ResolvedProjectDestination, resolve_project_run_destination,
};

use crate::{
    context::ProjectContext,
    render::redaction::{redact_exact, redact_uri_userinfo},
    suggestions,
};

const DESTINATION_URI_SHAPES: &[&str] = &[
    "duckdb://path",
    "parquet://root",
    "postgres://secret://env/NAME",
];

pub(crate) struct EnvironmentDestination {
    pub destination: ResolvedProjectDestination,
    pub secret_redaction: Option<String>,
}

pub(crate) fn resolve_environment_destination(
    registry: &cdf_runtime::DestinationRegistry,
    context: &ProjectContext,
    target: &TargetName,
) -> Result<EnvironmentDestination, CdfError> {
    resolve_selected_destination_with_services(registry, context, target, None, None)
}

pub(crate) fn resolve_selected_destination(
    registry: &cdf_runtime::DestinationRegistry,
    context: &ProjectContext,
    target: &TargetName,
    destination_uri: Option<&str>,
) -> Result<EnvironmentDestination, CdfError> {
    resolve_selected_destination_with_services(registry, context, target, destination_uri, None)
}

pub(crate) fn resolve_selected_destination_with_services(
    registry: &cdf_runtime::DestinationRegistry,
    context: &ProjectContext,
    target: &TargetName,
    destination_uri: Option<&str>,
    services: Option<&cdf_runtime::ExecutionServices>,
) -> Result<EnvironmentDestination, CdfError> {
    let secret_provider = context.secret_provider();
    let mut destination_context = ProjectResolutionContext::for_project_run(&context.root, target)
        .with_environment_name(&context.environment.name)
        .with_destination_policy(&context.environment.destination_policy)
        .with_secret_provider(&secret_provider);
    if let Some(services) = services {
        destination_context = destination_context.with_execution_services(services);
    }
    let uri = destination_uri.unwrap_or(context.environment.destination.as_str());
    let destination = resolve_project_run_destination(registry, uri, &destination_context)?;
    let secret_redaction = destination.secret_redaction().map(str::to_owned);
    Ok(EnvironmentDestination {
        destination,
        secret_redaction,
    })
}

pub(crate) fn redact_error_value(mut error: CdfError, secret: Option<&str>) -> CdfError {
    error.message = redact_uri_userinfo(&error.message);
    if let Some(secret) = secret
        && !secret.is_empty()
    {
        error.message = redact_exact(&error.message, Some(secret));
    }
    error
}

pub(crate) fn redact_destination_uri(value: &str) -> String {
    redact_uri_userinfo(value)
}

pub(crate) fn destination_error_suggestions(
    context: &ProjectContext,
    requested_destination: Option<&str>,
) -> Vec<String> {
    let mut values = Vec::new();
    if let Some(requested_destination) = requested_destination {
        values.extend(
            suggestions::nearest(
                requested_destination,
                context.config.environments.keys().cloned(),
            )
            .into_iter()
            .map(|environment| format!("--env {environment}")),
        );
    }

    for shape in DESTINATION_URI_SHAPES {
        if values.len() >= 3 {
            break;
        }
        values.push((*shape).to_owned());
    }
    values
}
