use cdf_kernel::{CdfError, TargetName};
use cdf_project::{
    ProjectResolutionContext, ResolvedProjectDestination, resolve_project_run_destination,
};

use crate::{context::ProjectContext, destination_uri::redact_error_value, output::CliError};

pub(super) struct SelectedDestination {
    destination: Option<ResolvedProjectDestination>,
    secret_redaction: Option<String>,
}

impl SelectedDestination {
    pub(super) fn from_context(
        context: &ProjectContext,
        command: &'static str,
        target: &TargetName,
    ) -> Result<Self, CliError> {
        let secret_provider = context.secret_provider();
        let destination_context = ProjectResolutionContext::for_project_run(&context.root, target)
            .with_environment_name(&context.environment.name)
            .with_destination_policy(&context.environment.destination_policy)
            .with_secret_provider(&secret_provider);
        let destination =
            resolve_project_run_destination(&context.environment.destination, &destination_context)
                .map_err(|error| resume_destination_resolution_error(error, command))?;
        let secret_redaction = destination.secret_redaction().map(str::to_owned);
        Ok(Self {
            destination: Some(destination),
            secret_redaction,
        })
    }

    pub(super) fn take(&mut self) -> Result<ResolvedProjectDestination, CliError> {
        self.destination
            .take()
            .ok_or_else(|| CdfError::internal("resume destination was already consumed").into())
    }

    pub(super) fn redact_error(&self, error: CdfError) -> CdfError {
        redact_error_value(error, self.secret_redaction.as_deref())
    }
}

fn resume_destination_resolution_error(error: CdfError, command: &'static str) -> CliError {
    if error
        .message
        .contains("no project destination driver registered")
        || error.message.contains("malformed or non-local")
        || error.message.contains("is missing a scheme")
    {
        CliError::not_supported(
            command,
            error.message,
            "registered project destination driver",
        )
    } else {
        error.into()
    }
}
