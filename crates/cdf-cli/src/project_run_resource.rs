use cdf_declarative::{
    CompiledResource, CompiledResourcePlan, FileResource, FileRuntimeDependencies,
    FileTransportFacade, RestResource, RestRuntimeDependencies, SqlResource,
    SqlRuntimeDependencies,
};
use cdf_kernel::QueryableResource;
use cdf_project::{ProjectRunSource, ResourceSourceKind, TrustPreset};

use crate::{context::ProjectContext, http_transport::ReqwestHttpTransport, output::CliError};

pub(crate) enum CliProjectRunSource {
    File(Box<FileResource>),
    Rest(Box<RestResource>),
    Sql(Box<SqlResource>),
    Python(Box<cdf_python::PythonResource>),
}

impl CliProjectRunSource {
    pub(crate) fn as_project_resource(&self) -> ProjectRunSource<'_> {
        match self {
            Self::File(resource) => ProjectRunSource::file(resource.as_ref()),
            Self::Rest(resource) => ProjectRunSource::rest(resource.as_ref()),
            Self::Sql(resource) => ProjectRunSource::sql(resource.as_ref()),
            Self::Python(resource) => ProjectRunSource::new(resource.as_ref()),
        }
    }

    pub(crate) fn as_queryable(&self) -> &dyn QueryableResource {
        match self {
            Self::File(resource) => resource.as_ref(),
            Self::Rest(resource) => resource.as_ref(),
            Self::Sql(resource) => resource.as_ref(),
            Self::Python(resource) => resource.as_ref(),
        }
    }
}

pub(crate) fn build_python_project_run_resource(
    context: &ProjectContext,
    resource_id: &str,
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
    let resource = cdf_python::PythonResource::load(
        &context.root,
        &uri,
        cdf_kernel::ResourceId::new(resource_id)?,
        trust,
    )
    .map_err(python_resource_error)?;
    Ok(Some(CliProjectRunSource::Python(Box::new(resource))))
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
) -> Result<CliProjectRunSource, CliError> {
    match resource.plan() {
        CompiledResourcePlan::Files(_) => Ok(CliProjectRunSource::File(Box::new(
            resource.to_file_resource(file_runtime_dependencies(context)?)?,
        ))),
        CompiledResourcePlan::Rest(_) => {
            let dependencies = RestRuntimeDependencies::new(ReqwestHttpTransport::new()?)
                .with_secret_provider(context.secret_provider());
            Ok(CliProjectRunSource::Rest(Box::new(
                resource.to_rest_resource(dependencies)?,
            )))
        }
        CompiledResourcePlan::Sql(_) => {
            let dependencies =
                SqlRuntimeDependencies::new().with_secret_provider(context.secret_provider());
            Ok(CliProjectRunSource::Sql(Box::new(
                resource.to_sql_resource(dependencies)?,
            )))
        }
    }
}

pub(crate) fn file_runtime_dependencies(
    context: &ProjectContext,
) -> Result<FileRuntimeDependencies, CliError> {
    let facade = FileTransportFacade::new()
        .with_http_transport(ReqwestHttpTransport::new()?)
        .with_secret_provider(context.secret_provider());
    Ok(FileRuntimeDependencies::new(facade))
}
