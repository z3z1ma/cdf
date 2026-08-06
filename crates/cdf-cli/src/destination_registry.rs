use cdf_kernel::Result;
use cdf_runtime::DestinationRegistry;

use crate::context::DestinationRuntime;
use crate::context::ProjectContext;

pub(crate) use cdf_builtin_drivers::builtin_destination_registry;

pub(crate) fn inspect_destination_artifacts(
    registry: &DestinationRegistry,
    context: &ProjectContext,
    uri: &str,
) -> Result<Vec<cdf_kernel::DestinationSheetArtifact>> {
    inspect_destination_artifacts_and_id(registry, context, uri).map(|(_, artifacts)| artifacts)
}

pub(crate) fn inspect_destination_artifacts_and_id(
    registry: &DestinationRegistry,
    context: &ProjectContext,
    uri: &str,
) -> Result<(String, Vec<cdf_kernel::DestinationSheetArtifact>)> {
    let resolution =
        cdf_runtime::DestinationResolutionContext::for_project_inspection(&context.root)
            .with_environment_name(&context.environment.name)
            .with_destination_policy(&context.environment.destination_policy);
    let inspection = registry.inspect(uri, &resolution)?;
    Ok((
        inspection.description.destination_id.to_string(),
        vec![inspection.sheet_artifact],
    ))
}

pub(crate) fn inspect_destination_runtime(
    registry: &DestinationRegistry,
    context: &ProjectContext,
) -> DestinationRuntime {
    inspect_destination_runtime_for_environment(registry, &context.root, &context.environment)
}

pub(crate) fn inspect_destination_runtime_for_environment(
    registry: &DestinationRegistry,
    root: &std::path::Path,
    environment: &cdf_project::EffectiveEnvironment,
) -> DestinationRuntime {
    let uri = &environment.destination;
    let resolution = cdf_runtime::DestinationResolutionContext::for_project_inspection(root)
        .with_environment_name(&environment.name)
        .with_destination_policy(&environment.destination_policy);
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
