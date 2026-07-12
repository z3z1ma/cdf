use cdf_kernel::Result;
use cdf_runtime::{FormatRegistry, SourceRegistry};
use cdf_source_files::{FileRuntimeDependencies, FileSourceDriver, FileTransportFacade};
use cdf_source_postgres::PostgresSourceDriver;
use cdf_source_rest::RestSourceDriver;

use crate::http_transport::ReqwestHttpTransport;

pub(crate) fn builtin_source_registry() -> Result<SourceRegistry> {
    let mut registry = SourceRegistry::new();
    registry.register(PostgresSourceDriver::new()?)?;
    registry.register(RestSourceDriver::new(|| {
        Ok(Box::new(ReqwestHttpTransport::new()?))
    })?)?;
    registry.register(FileSourceDriver::new(|secrets, execution| {
        Ok(FileRuntimeDependencies::new(
            FileTransportFacade::new()
                .with_http_transport(ReqwestHttpTransport::new()?)
                .with_shared_secret_provider(secrets)
                .with_execution_services(execution.clone()),
            execution,
            builtin_format_registry()?,
        ))
    })?)?;
    Ok(registry)
}

pub(crate) fn builtin_format_registry() -> Result<std::sync::Arc<FormatRegistry>> {
    let mut registry = FormatRegistry::default();
    registry.register(std::sync::Arc::new(
        cdf_format_parquet::ParquetFormatDriver::new()?,
    ))?;
    Ok(std::sync::Arc::new(registry))
}
