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
    source_plan: Option<cdf_runtime::CompiledSourcePlan>,
}

impl CliProjectRunSource {
    fn new(resource: impl QueryableResource + 'static) -> Self {
        Self {
            resource: Arc::new(resource),
            source_plan: None,
        }
    }

    fn from_shared(
        resource: Arc<dyn QueryableResource>,
        source_plan: cdf_runtime::CompiledSourcePlan,
    ) -> Self {
        Self {
            resource,
            source_plan: Some(source_plan),
        }
    }

    pub(crate) fn as_project_resource(&self) -> ProjectRunSource<'_> {
        ProjectRunSource::new(self.resource.as_ref())
    }

    pub(crate) fn as_queryable(&self) -> &dyn QueryableResource {
        self.resource.as_ref()
    }

    pub(crate) fn source_plan(&self) -> Option<&cdf_runtime::CompiledSourcePlan> {
        self.source_plan.as_ref()
    }
}

fn build_python_project_run_resource(
    context: &ProjectContext,
    resource_id: &str,
    execution: Option<&cdf_runtime::ExecutionServices>,
) -> Result<Option<CliProjectRunSource>, CliError> {
    let Some(mapping) = context.python_resource_mapping(resource_id) else {
        return Ok(None);
    };
    if resource_id.contains('*') {
        return Err(python_resource_error(cdf_kernel::CdfError::contract(
            "Python resource mappings must use one exact resource id, not a wildcard",
        )));
    }
    let ResourceSourceKind::Python { uri } = mapping.source_kind() else {
        unreachable!("python_resource_mapping returned a non-Python mapping");
    };
    let interpreter = context
        .config
        .python
        .interpreter
        .as_deref()
        .ok_or_else(|| {
            python_resource_error(cdf_kernel::CdfError::contract(
                "python.interpreter is required for Python plan, preview, and run",
            ))
        })?;
    let configured = if std::path::Path::new(interpreter).is_absolute() {
        std::path::PathBuf::from(interpreter)
    } else {
        context.root.join(interpreter)
    };
    let configured = configured.canonicalize().map_err(|error| {
        python_resource_error(cdf_kernel::CdfError::contract(format!(
            "configured Python interpreter is missing or inaccessible at {}: {error}",
            configured.display()
        )))
    })?;
    cdf_python::validate_attached_interpreter(
        configured,
        context.config.python.require_free_threaded.unwrap_or(false),
    )
    .map_err(python_resource_error)?;
    let trust = mapping
        .trust
        .as_ref()
        .or(context.config.defaults.trust.as_ref())
        .map(trust_level)
        .unwrap_or(cdf_kernel::TrustLevel::Experimental);
    let mut resource = cdf_python::PythonResource::load(
        &context.root,
        &uri,
        cdf_kernel::ResourceId::new(resource_id)?,
        trust,
    )
    .map_err(python_resource_error)?;
    if let Some(execution) = execution {
        resource = resource
            .with_execution_services(execution.clone())
            .map_err(python_resource_error)?;
    }
    Ok(Some(CliProjectRunSource::new(resource)))
}

pub(crate) fn build_project_resource_for_inspection(
    context: &ProjectContext,
    resource_id: &str,
) -> Result<Option<CliProjectRunSource>, CliError> {
    build_python_project_run_resource(context, resource_id, None)
}

pub(crate) fn prepare_runtime_resource_for_cli(
    destinations: &cdf_runtime::DestinationRegistry,
    context: &ProjectContext,
    resource_id: &str,
    no_pin: bool,
    execution: Option<&cdf_runtime::ExecutionServices>,
) -> Result<PreparedRuntimeResourceForCli, CliError> {
    if let Some(resource) = build_python_project_run_resource(context, resource_id, execution)? {
        return Ok(PreparedRuntimeResourceForCli {
            resource,
            schema_snapshot: None,
        });
    }
    let compiled = context.resource(resource_id)?;
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

fn python_resource_error(mut error: cdf_kernel::CdfError) -> CliError {
    if !error.message.contains("cdf doctor") {
        error
            .message
            .push_str("; run `cdf doctor` for interpreter diagnostics");
    }
    CliError::mapped(error, crate::error_catalog::PYTHON_RESOURCE)
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
    let resolution =
        cdf_runtime::SourceResolutionContext::new(&context.root, Arc::new(secrets), execution)
            .with_prepared_payloads(prepared_payloads);
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
    let resolution = cdf_runtime::SourceResolutionContext::new(
        &context.root,
        Arc::new(context.secret_provider()),
        execution,
    )
    .with_prepared_payloads(prepared_payloads);
    cdf_project::discover_resource_schema_with_source_registry(
        resource,
        &registry,
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
    let resolution = cdf_runtime::SourceResolutionContext::new(
        &context.root,
        Arc::new(context.secret_provider()),
        execution,
    );
    cdf_project::preflight_fixed_resource_schema_with_source_registry(
        &context.root,
        resource,
        &registry,
        source_plan,
        &resolution,
        options,
    )
}
