use cdf_dest_duckdb::DuckDbRuntimeDriver;
use cdf_dest_parquet::ParquetRuntimeDriver;
use cdf_dest_postgres::PostgresRuntimeDriver;
use cdf_kernel::Result;
use cdf_runtime::DestinationRegistry;

pub(crate) fn builtin_destination_registry() -> Result<DestinationRegistry> {
    let mut registry = DestinationRegistry::new();
    registry.register(DuckDbRuntimeDriver)?;
    registry.register(ParquetRuntimeDriver)?;
    registry.register(PostgresRuntimeDriver)?;
    Ok(registry)
}
