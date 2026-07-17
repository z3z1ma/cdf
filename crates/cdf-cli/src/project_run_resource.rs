use cdf_declarative::CompiledResource;
use cdf_kernel::QueryableResource;
use cdf_project::{ProjectRunSource, ResourceSourceKind, TrustPreset};
use std::sync::Arc;

use crate::{context::ProjectContext, output::CliError};

pub(crate) struct PreparedRuntimeResourceForCli {
    pub(crate) resource: CliProjectRunSource,
    pub(crate) schema_snapshot: Option<crate::reports::SchemaSnapshotActionReport>,
}

pub(crate) struct CliProjectRunSource {
    resource: Arc<dyn QueryableResource>,
    source_plan: cdf_runtime::CompiledSourcePlan,
}

impl CliProjectRunSource {
    fn from_shared(
        resource: Arc<dyn QueryableResource>,
        source_plan: cdf_runtime::CompiledSourcePlan,
    ) -> Self {
        Self {
            resource,
            source_plan,
        }
    }

    pub(crate) fn as_project_resource(&self) -> ProjectRunSource<'_> {
        ProjectRunSource::new(self.resource.as_ref())
    }

    pub(crate) fn as_queryable(&self) -> &dyn QueryableResource {
        self.resource.as_ref()
    }

    pub(crate) fn source_plan(&self) -> &cdf_runtime::CompiledSourcePlan {
        &self.source_plan
    }
}

fn compile_project_source_reference(
    context: &ProjectContext,
    resource_id: &str,
) -> Result<Option<CompiledResource>, CliError> {
    let Some(mapping) = context.source_reference_mapping(resource_id) else {
        return Ok(None);
    };
    if resource_id.contains('*') {
        return Err(source_reference_error(cdf_kernel::CdfError::contract(
            "source reference mappings must use one exact resource id, not a wildcard",
        )));
    }
    let ResourceSourceKind::Reference { uri } = mapping.source_kind() else {
        unreachable!("source_reference_mapping returned a declarative mapping");
    };
    let registry = crate::source_registry::builtin_source_registry()?;
    let driver = registry.driver_for_uri(&uri)?;
    let project_options = context
        .config
        .driver_options
        .get(driver.descriptor().driver_id.as_str())
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let trust = mapping
        .trust
        .as_ref()
        .or(context.config.defaults.trust.as_ref())
        .map(trust_level)
        .unwrap_or(cdf_kernel::TrustLevel::Experimental);
    let source_plan = registry
        .compile_reference(cdf_runtime::SourceReferenceCompileRequest {
            uri,
            resource_id: cdf_kernel::ResourceId::new(resource_id)?,
            project_root: context.root.clone(),
            trust_level: trust,
            freshness: mapping
                .freshness
                .as_ref()
                .and_then(|freshness| freshness.alert_after)
                .map(|alert_after| cdf_kernel::FreshnessSpec {
                    max_age_ms: alert_after.millis(),
                }),
            project_options,
        })
        .map_err(source_reference_error)?;
    let (source_name, resource_name) = resource_id.split_once('.').ok_or_else(|| {
        source_reference_error(cdf_kernel::CdfError::contract(
            "source reference resource ids must use `<source>.<resource>`",
        ))
    })?;
    Ok(Some(CompiledResource::from_compiled_source(
        source_name,
        resource_name,
        Some(context.root.clone()),
        source_plan,
    )?))
}

pub(crate) fn build_project_resource_for_inspection(
    context: &ProjectContext,
    resource_id: &str,
) -> Result<Option<CompiledResource>, CliError> {
    compile_project_source_reference(context, resource_id)
}

pub(crate) fn prepare_runtime_resource_for_cli(
    destinations: &cdf_runtime::DestinationRegistry,
    context: &ProjectContext,
    resource_id: &str,
    no_pin: bool,
    execution: Option<&cdf_runtime::ExecutionServices>,
) -> Result<PreparedRuntimeResourceForCli, CliError> {
    let referenced = compile_project_source_reference(context, resource_id)?;
    let compiled = match referenced.as_ref() {
        Some(resource) => resource,
        None => context.resource(resource_id)?,
    };
    let prepared = crate::scan_command::prepare_resource_schema_for_cli(
        destinations,
        context,
        compiled,
        no_pin,
        execution,
    )?;
    Ok(PreparedRuntimeResourceForCli {
        resource: build_project_run_resource(
            context,
            &prepared.resource,
            prepared.source_plan,
            execution,
            prepared.prepared_payloads,
        )?,
        schema_snapshot: prepared.schema_snapshot,
    })
}

fn trust_level(trust: &TrustPreset) -> cdf_kernel::TrustLevel {
    match trust {
        TrustPreset::Experimental => cdf_kernel::TrustLevel::Experimental,
        TrustPreset::Governed => cdf_kernel::TrustLevel::Governed,
        TrustPreset::Financial => cdf_kernel::TrustLevel::Financial,
        TrustPreset::Serving => cdf_kernel::TrustLevel::Serving,
    }
}

fn source_reference_error(mut error: cdf_kernel::CdfError) -> CliError {
    if !error.message.contains("cdf doctor") {
        error
            .message
            .push_str("; run `cdf doctor` for source-driver diagnostics");
    }
    CliError::mapped(error, crate::error_catalog::SOURCE_REFERENCE)
}

pub(crate) fn build_project_run_resource(
    context: &ProjectContext,
    resource: &CompiledResource,
    source_plan: cdf_runtime::CompiledSourcePlan,
    execution: Option<&cdf_runtime::ExecutionServices>,
    prepared_payloads: cdf_runtime::PreparedSourcePayloads,
) -> Result<CliProjectRunSource, CliError> {
    let execution = execution.ok_or_else(|| {
        cdf_kernel::CdfError::internal("runtime source resolution requires execution services")
    })?;
    let registry = crate::source_registry::builtin_source_registry()?;
    source_plan.validate_schema_authority(
        resource.descriptor(),
        resource.schema().as_ref(),
        resource.effective_schema_runtime(),
        resource.baseline_observation_schema_catalog(),
    )?;
    let secrets = context.secret_provider();
    let resolution = cdf_runtime::SourceResolutionContext::new(
        &context.root,
        Arc::new(secrets),
        execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_prepared_payloads(prepared_payloads)
    .with_driver_options(context.config.driver_options.clone());
    Ok(CliProjectRunSource::from_shared(
        registry.resolve(&source_plan, &resolution)?,
        source_plan,
    ))
}

pub(crate) fn compile_source_plan_for_cli(
    resource: &CompiledResource,
) -> cdf_kernel::Result<cdf_runtime::CompiledSourcePlan> {
    resource.source_plan().validate()?;
    Ok(resource.source_plan().clone())
}

pub(crate) fn discover_source_schema_for_cli(
    context: &ProjectContext,
    resource: &CompiledResource,
    execution: &cdf_runtime::ExecutionServices,
    prepared_payloads: cdf_runtime::PreparedSourcePayloads,
    options: cdf_project::SchemaDiscoveryExecutionOptions,
) -> cdf_kernel::Result<cdf_project::ResourceSchemaDiscoveryArtifacts> {
    let source_plan = compile_source_plan_for_cli(resource)?;
    discover_source_schema_with_plan_for_cli(
        context,
        resource,
        &source_plan,
        execution,
        prepared_payloads,
        options,
    )
}

pub(crate) fn discover_source_schema_with_plan_for_cli(
    context: &ProjectContext,
    resource: &CompiledResource,
    source_plan: &cdf_runtime::CompiledSourcePlan,
    execution: &cdf_runtime::ExecutionServices,
    prepared_payloads: cdf_runtime::PreparedSourcePayloads,
    options: cdf_project::SchemaDiscoveryExecutionOptions,
) -> cdf_kernel::Result<cdf_project::ResourceSchemaDiscoveryArtifacts> {
    let registry = crate::source_registry::builtin_source_registry()?;
    let cancellation = options.cancellation();
    let resolution = cdf_runtime::SourceResolutionContext::new(
        &context.root,
        Arc::new(context.secret_provider()),
        execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_cancellation(cancellation)
    .with_prepared_payloads(prepared_payloads)
    .with_driver_options(context.config.driver_options.clone());
    cdf_project::discover_resource_schema_with_source_registry(
        resource,
        registry,
        source_plan,
        &resolution,
        options,
    )
}

pub(crate) fn preflight_fixed_source_schema_with_plan_for_cli(
    context: &ProjectContext,
    resource: &CompiledResource,
    source_plan: &cdf_runtime::CompiledSourcePlan,
    execution: &cdf_runtime::ExecutionServices,
    options: cdf_project::SchemaDiscoveryExecutionOptions,
) -> cdf_kernel::Result<cdf_project::ResourceSchemaDiscoveryArtifacts> {
    let registry = crate::source_registry::builtin_source_registry()?;
    let cancellation = options.cancellation();
    let resolution = cdf_runtime::SourceResolutionContext::new(
        &context.root,
        Arc::new(context.secret_provider()),
        execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_cancellation(cancellation)
    .with_driver_options(context.config.driver_options.clone());
    cdf_project::preflight_fixed_resource_schema_with_source_registry(
        &context.root,
        resource,
        registry,
        source_plan,
        &resolution,
        options,
    )
}
