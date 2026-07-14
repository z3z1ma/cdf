use cdf_dest_duckdb::DuckDbRuntimeDriver;
use cdf_dest_parquet::ParquetRuntimeDriver;
use cdf_dest_postgres::PostgresRuntimeDriver;
use cdf_kernel::Result;
use cdf_runtime::DestinationRegistry;

use crate::context::DestinationRuntime;
use crate::context::ProjectContext;

pub(crate) fn builtin_destination_registry() -> Result<DestinationRegistry> {
    let mut registry = DestinationRegistry::new();
    registry.register(DuckDbRuntimeDriver)?;
    registry.register(ParquetRuntimeDriver)?;
    registry.register(PostgresRuntimeDriver)?;
    Ok(registry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_destination_catalog_artifact_matches_the_product_registry() {
        let registry = builtin_destination_registry().unwrap();
        let context = cdf_runtime::DestinationResolutionContext::for_project_inspection(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")),
        );
        let actual = [
            "duckdb:///tmp/cdf-envelope.duckdb",
            "parquet:///tmp/cdf-envelope-parquet",
            "postgres://localhost/cdf_envelope",
        ]
        .into_iter()
        .map(|uri| {
            let inspection = registry.inspect(uri, &context).unwrap();
            serde_json::json!({
                "destination_id": inspection.description.destination_id.as_str(),
                "runtime": inspection.runtime,
            })
        })
        .collect::<Vec<_>>();
        let expected: Vec<serde_json::Value> = serde_json::from_str(include_str!(
            "../../cdf-benchmarks/fixtures/first-party-destination-catalog.json"
        ))
        .unwrap();
        assert_eq!(actual, expected);
    }
}

pub(crate) fn inspect_destination_artifacts(
    registry: &DestinationRegistry,
    context: &ProjectContext,
    uri: &str,
) -> Result<Vec<cdf_kernel::DestinationSheetArtifact>> {
    let resolution =
        cdf_runtime::DestinationResolutionContext::for_project_inspection(&context.root)
            .with_environment_name(&context.environment.name)
            .with_destination_policy(&context.environment.destination_policy);
    Ok(vec![registry.inspect(uri, &resolution)?.sheet_artifact])
}

pub(crate) fn inspect_destination_runtime(
    registry: &DestinationRegistry,
    context: &ProjectContext,
) -> DestinationRuntime {
    let uri = &context.environment.destination;
    let resolution =
        cdf_runtime::DestinationResolutionContext::for_project_inspection(&context.root)
            .with_environment_name(&context.environment.name)
            .with_destination_policy(&context.environment.destination_policy);
    let inspection = match registry.inspect(uri, &resolution) {
        Ok(inspection) => inspection,
        Err(error) => return unsupported_runtime(uri, error.to_string()),
    };
    let health = match registry.health(uri, &resolution) {
        Ok(health) => health,
        Err(error) => vec![cdf_runtime::DestinationHealthResult {
            probe_id: "destination".to_owned(),
            status: cdf_runtime::DestinationHealthStatus::Failed,
            message: error.to_string(),
            details: Default::default(),
        }],
    };
    DestinationRuntime {
        kind: inspection.description.destination_id.to_string(),
        destination_id: Some(inspection.description.destination_id.to_string()),
        label: Some(inspection.description.label),
        schemes: inspection
            .description
            .schemes
            .iter()
            .map(|scheme| (*scheme).to_owned())
            .collect(),
        sheet: Some(inspection.sheet_artifact),
        capabilities: Some(inspection.runtime),
        health,
        error: None,
    }
}

fn unsupported_runtime(uri: &str, reason: String) -> DestinationRuntime {
    DestinationRuntime {
        kind: "unsupported".to_owned(),
        destination_id: None,
        label: Some(uri.to_owned()),
        schemes: Vec::new(),
        sheet: None,
        capabilities: None,
        health: Vec::new(),
        error: Some(reason),
    }
}
