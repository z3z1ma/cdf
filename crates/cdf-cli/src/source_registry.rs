use cdf_kernel::Result;
use cdf_runtime::SourceRegistry;
use cdf_source_postgres::PostgresSourceDriver;

pub(crate) fn builtin_source_registry() -> Result<SourceRegistry> {
    let mut registry = SourceRegistry::new();
    registry.register(PostgresSourceDriver::new()?)?;
    Ok(registry)
}
