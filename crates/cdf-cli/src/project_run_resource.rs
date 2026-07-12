use cdf_declarative::{CompiledResource, FileRuntimeDependencies, FileTransportFacade};
use cdf_kernel::QueryableResource;
use cdf_project::{ProjectRunSource, ResourceSourceKind, TrustPreset};
use std::sync::Arc;

use crate::{context::ProjectContext, http_transport::ReqwestHttpTransport, output::CliError};

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
) -> Result<CliProjectRunSource, CliError> {
    match build_python_project_run_resource(context, resource_id, None)? {
        Some(resource) => Ok(resource),
        None => Ok(CliProjectRunSource::new(
            context.resource(resource_id)?.clone(),
        )),
    }
}

pub(crate) fn prepare_runtime_resource_for_cli(
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
    let prepared = crate::scan_command::prepare_discover_resource_for_cli(
        context, compiled, no_pin, execution,
    )?;
    Ok(PreparedRuntimeResourceForCli {
        resource: build_project_run_resource(context, &prepared.resource, execution)?,
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
    execution: Option<&cdf_runtime::ExecutionServices>,
) -> Result<CliProjectRunSource, CliError> {
    let execution = execution.ok_or_else(|| {
        cdf_kernel::CdfError::internal("runtime source resolution requires execution services")
    })?;
    let plan = resource.source_plan().ok_or_else(|| {
        cdf_kernel::CdfError::contract(format!(
            "resource `{}` has no executable source driver plan",
            resource.descriptor().resource_id
        ))
    })?;
    let registry = crate::source_registry::builtin_source_registry()?;
    let secrets = context.secret_provider();
    let resolution =
        cdf_runtime::SourceResolutionContext::new(&context.root, Arc::new(secrets), execution);
    Ok(CliProjectRunSource::from_shared(
        registry.resolve(plan, &resolution)?,
        plan.clone(),
    ))
}

pub(crate) fn file_runtime_dependencies(
    context: &ProjectContext,
    execution: Option<&cdf_runtime::ExecutionServices>,
) -> Result<FileRuntimeDependencies, CliError> {
    let mut facade = FileTransportFacade::new()
        .with_http_transport(ReqwestHttpTransport::new()?)
        .with_secret_provider(context.secret_provider());
    if let Some(execution) = execution {
        facade = facade.with_execution_services(execution.clone());
    }
    let execution = execution.ok_or_else(|| {
        cdf_kernel::CdfError::internal("file runtime dependencies require execution services")
    })?;
    Ok(FileRuntimeDependencies::new(
        facade,
        execution.clone(),
        crate::source_registry::builtin_format_registry()?,
        crate::source_registry::builtin_transform_registry()?,
    ))
}
