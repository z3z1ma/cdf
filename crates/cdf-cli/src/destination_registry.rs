use cdf_dest_duckdb::DuckDbRuntimeDriver;
use cdf_dest_parquet::ParquetRuntimeDriver;
use cdf_dest_postgres::PostgresRuntimeDriver;
use cdf_kernel::Result;
use cdf_runtime::DestinationRegistry;

use crate::context::ProjectContext;

pub(crate) fn builtin_destination_registry() -> Result<DestinationRegistry> {
    let mut registry = DestinationRegistry::new();
    registry.register(DuckDbRuntimeDriver)?;
    registry.register(ParquetRuntimeDriver)?;
    registry.register(PostgresRuntimeDriver)?;
    Ok(registry)
}

pub(crate) fn inspect_destination_artifacts(
    context: &ProjectContext,
    uri: &str,
) -> Result<Vec<cdf_kernel::DestinationSheetArtifact>> {
    let registry = builtin_destination_registry()?;
    let resolution =
        cdf_runtime::DestinationResolutionContext::for_project_inspection(&context.root)
            .with_environment_name(&context.environment.name)
            .with_destination_policy(&context.environment.destination_policy);
    Ok(vec![registry.inspect(uri, &resolution)?.sheet_artifact])
}
