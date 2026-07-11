use cdf_kernel::{CdfError, TargetName};
use cdf_project::ResolvedProjectDestination;

use crate::{
    context::ProjectContext,
    destination_uri::{
        destination_error_suggestions, redact_error_value,
        resolve_selected_destination_with_services,
    },
    error_catalog,
    output::CliError,
};

pub(super) struct SelectedDestination {
    destination: Option<ResolvedProjectDestination>,
    secret_redaction: Option<String>,
}

impl SelectedDestination {
    pub(super) fn from_context(
        context: &ProjectContext,
        command: &'static str,
        target: &TargetName,
        execution: &cdf_runtime::ExecutionServices,
    ) -> Result<Self, CliError> {
        let resolved =
            resolve_selected_destination_with_services(context, target, None, Some(execution))
                .map_err(|error| resume_destination_resolution_error(context, error, command))?;
        Ok(Self {
            destination: Some(resolved.destination),
            secret_redaction: resolved.secret_redaction,
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

fn resume_destination_resolution_error(
    context: &ProjectContext,
    error: CdfError,
    command: &'static str,
) -> CliError {
    let error = redact_error_value(error, None);
    if error
        .message
        .contains("no project destination driver registered")
        || error.message.contains("malformed or non-local")
        || error.message.contains("is missing a scheme")
    {
        CliError::not_supported_with(
            command,
            error.message,
            "registered project destination driver",
            error_catalog::DESTINATION_NOT_SUPPORTED,
        )
        .with_suggestions(destination_error_suggestions(context, None))
    } else {
        error.into()
    }
}
