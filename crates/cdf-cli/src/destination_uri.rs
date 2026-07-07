use cdf_http::SecretProvider;
use cdf_kernel::CdfError;
use std::path::PathBuf;

use crate::{context::ProjectContext, output::CliError};

pub(crate) fn parquet_filesystem_root(
    context: &ProjectContext,
    uri: &str,
    command: &'static str,
) -> Result<PathBuf, CliError> {
    let Some(raw_root) = uri.strip_prefix("parquet://") else {
        return Err(CliError::not_supported(
            command,
            format!("destination URI `{uri}` is unsupported; expected parquet://root"),
            "filesystem Parquet destination root",
        ));
    };
    if raw_root.trim().is_empty() || raw_root.contains("://") {
        return Err(CliError::not_supported(
            command,
            format!("destination URI `{uri}` is malformed or non-local; expected parquet://root"),
            "filesystem Parquet destination root",
        ));
    }
    let root = PathBuf::from(raw_root);
    Ok(if root.is_absolute() {
        root
    } else {
        context.root.join(root)
    })
}

pub(crate) fn postgres_database_url(
    context: &ProjectContext,
    uri: &str,
    command: &'static str,
) -> Result<(String, bool), CliError> {
    let Some(raw) = uri.strip_prefix("postgres://") else {
        return Err(CliError::not_supported(
            command,
            format!("destination URI `{uri}` is unsupported; expected postgres://..."),
            "Postgres destination",
        ));
    };
    if raw.trim().is_empty() {
        return Err(CliError::not_supported(
            command,
            "Postgres destination URI is malformed; expected postgres://database-url or postgres://secret://provider/key",
            "Postgres database URL",
        ));
    }
    if raw.starts_with("secret://") {
        let secret = cdf_project::SecretRef::new(raw.to_owned())?;
        let provider = context.secret_provider();
        let value = provider.resolve(&secret.to_secret_uri()?)?;
        return Ok((value.as_str()?.to_owned(), true));
    }
    Ok((uri.to_owned(), false))
}

pub(crate) fn redact_error_value(mut error: CdfError, secret: Option<&str>) -> CdfError {
    if let Some(secret) = secret
        && !secret.is_empty()
    {
        error.message = error.message.replace(secret, "[REDACTED]");
    }
    error
}
