use std::{path::Path, sync::Arc};

use cdf_declarative::CompiledResource;
use cdf_http::{SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{CdfError, QueryableResource, Result};
use cdf_runtime::{ByteTransformRegistry, FormatRegistry, SourceRegistry, SourceResolutionContext};
use cdf_source_files::{FileRuntimeDependencies, FileSourceDriver, FileTransportFacade};

struct NoSecrets;

impl SecretProvider for NoSecrets {
    fn resolve(&self, uri: &SecretUri) -> Result<SecretValue> {
        Err(CdfError::auth(format!(
            "conformance fixture has no secret for {uri}"
        )))
    }
}

pub(crate) fn resolve_local_file(
    resource: &CompiledResource,
    project_root: &Path,
) -> Result<Arc<dyn QueryableResource>> {
    let execution = crate::test_execution_services();
    let mut registry = SourceRegistry::new();
    registry.register(FileSourceDriver::new(|secrets, execution| {
        Ok(FileRuntimeDependencies::new(
            FileTransportFacade::new()
                .with_shared_secret_provider(secrets)
                .with_execution_services(execution.clone()),
            execution,
            Arc::new(FormatRegistry::default()),
            Arc::new(ByteTransformRegistry::default()),
        ))
    })?)?;
    let plan = resource.source_plan().ok_or_else(|| {
        CdfError::contract(format!(
            "conformance resource `{}` has no executable source driver plan",
            resource.descriptor().resource_id
        ))
    })?;
    let context = SourceResolutionContext::new(project_root, Arc::new(NoSecrets), &execution);
    registry.resolve(plan, &context)
}
