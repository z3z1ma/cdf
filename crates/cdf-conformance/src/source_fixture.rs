use std::{path::Path, sync::Arc};

#[cfg(test)]
use std::collections::BTreeMap;

use cdf_declarative::CompiledResource;
use cdf_http::{SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{CdfError, QueryableResource, Result};
use cdf_runtime::{
    ByteTransformRegistry, CompiledSourcePlan, FormatRegistry, SourceRegistry,
    SourceResolutionContext,
};
use cdf_source_files::{FileRuntimeDependencies, FileSourceDriver, FileTransportFacade};

struct NoSecrets;

impl SecretProvider for NoSecrets {
    fn resolve(&self, uri: &SecretUri) -> Result<SecretValue> {
        Err(CdfError::auth(format!(
            "conformance fixture has no secret for {uri}"
        )))
    }
}

pub(crate) struct ResolvedSourceFixture {
    resource: Arc<dyn QueryableResource>,
    source_plan: CompiledSourcePlan,
    #[cfg(test)]
    execution: cdf_runtime::ExecutionServices,
}

impl ResolvedSourceFixture {
    pub(crate) fn resolve(
        compiled: &CompiledResource,
        registry: &SourceRegistry,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Self> {
        Ok(Self {
            resource: registry.resolve(compiled.source_plan(), context)?,
            source_plan: compiled.source_plan().clone(),
            #[cfg(test)]
            execution: context.execution().clone(),
        })
    }

    pub(crate) fn queryable(&self) -> &dyn QueryableResource {
        self.resource.as_ref()
    }

    pub(crate) fn bind_plan(&self, plan: cdf_engine::EnginePlan) -> Result<cdf_engine::EnginePlan> {
        plan.bind_compiled_source(&self.source_plan)
    }

    #[cfg(test)]
    pub(crate) fn execution(&self) -> &cdf_runtime::ExecutionServices {
        &self.execution
    }
}

pub(crate) fn resolve_local_file(
    resource: &CompiledResource,
    project_root: &Path,
) -> Result<ResolvedSourceFixture> {
    let execution = crate::test_execution_services();
    let registry = local_file_registry()?;
    let context = SourceResolutionContext::new(
        project_root,
        Arc::new(NoSecrets),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    ResolvedSourceFixture::resolve(resource, &registry, &context)
}

#[cfg(test)]
pub(crate) fn resolve_with_registry(
    resource: &CompiledResource,
    registry: &SourceRegistry,
    project_root: &Path,
    driver_options: BTreeMap<String, serde_json::Value>,
) -> Result<ResolvedSourceFixture> {
    let execution = crate::test_execution_services();
    let context = SourceResolutionContext::new(
        project_root,
        Arc::new(NoSecrets),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_driver_options(driver_options);
    ResolvedSourceFixture::resolve(resource, registry, &context)
}

pub(crate) fn local_file_registry() -> Result<SourceRegistry> {
    let mut formats = FormatRegistry::default();
    formats.register(Arc::new(cdf_format_json::NdjsonFormatDriver::new()?))?;
    let formats = Arc::new(formats);
    let mut registry = SourceRegistry::new();
    let compile_formats = Arc::clone(&formats);
    registry.register(FileSourceDriver::new(
        compile_formats,
        move |secrets, execution, egress| {
            Ok(FileRuntimeDependencies::new(
                FileTransportFacade::new()
                    .with_shared_secret_provider(secrets)
                    .with_execution_services(execution.clone()),
                execution,
                formats.clone(),
                Arc::new(ByteTransformRegistry::default()),
                egress,
            ))
        },
    )?)?;
    Ok(registry)
}
