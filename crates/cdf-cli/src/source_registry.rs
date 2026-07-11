use cdf_kernel::Result;
use cdf_runtime::SourceRegistry;
use cdf_source_postgres::PostgresSourceDriver;
use cdf_source_rest::RestSourceDriver;

use crate::http_transport::ReqwestHttpTransport;

pub(crate) fn builtin_source_registry() -> Result<SourceRegistry> {
    let mut registry = SourceRegistry::new();
    registry.register(PostgresSourceDriver::new()?)?;
    registry.register(RestSourceDriver::new(|| {
        Ok(Box::new(ReqwestHttpTransport::new()?))
    })?)?;
    Ok(registry)
}
