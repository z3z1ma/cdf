use cdf_kernel::{CdfError, TargetName};
use cdf_project::{
    ProjectResolutionContext, ResolvedProjectDestination, resolve_project_run_destination,
};

use crate::context::ProjectContext;

pub(crate) struct EnvironmentDestination {
    pub destination: ResolvedProjectDestination,
    pub secret_redaction: Option<String>,
}

pub(crate) fn resolve_environment_destination(
    context: &ProjectContext,
    target: &TargetName,
) -> Result<EnvironmentDestination, CdfError> {
    resolve_selected_destination(context, target, None)
}

pub(crate) fn resolve_selected_destination(
    context: &ProjectContext,
    target: &TargetName,
    destination_uri: Option<&str>,
) -> Result<EnvironmentDestination, CdfError> {
    let secret_provider = context.secret_provider();
    let destination_context = ProjectResolutionContext::for_project_run(&context.root, target)
        .with_environment_name(&context.environment.name)
        .with_destination_policy(&context.environment.destination_policy)
        .with_secret_provider(&secret_provider);
    let uri = destination_uri.unwrap_or(context.environment.destination.as_str());
    let destination = resolve_project_run_destination(uri, &destination_context)?;
    let secret_redaction = destination.secret_redaction().map(str::to_owned);
    Ok(EnvironmentDestination {
        destination,
        secret_redaction,
    })
}

pub(crate) fn redact_error_value(mut error: CdfError, secret: Option<&str>) -> CdfError {
    if let Some(secret) = secret
        && !secret.is_empty()
    {
        error.message = error.message.replace(secret, "[REDACTED]");
    }
    error
}
