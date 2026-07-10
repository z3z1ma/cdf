use cdf_declarative::{
    CompiledResource, CompiledResourcePlan, FileResource, FileRuntimeDependencies,
    FileTransportFacade, RestResource, RestRuntimeDependencies, SqlResource,
    SqlRuntimeDependencies,
};
use cdf_kernel::QueryableResource;
use cdf_project::ProjectRunSource;

use crate::{context::ProjectContext, http_transport::ReqwestHttpTransport, output::CliError};

pub(crate) enum CliProjectRunSource {
    File(Box<FileResource>),
    Rest(Box<RestResource>),
    Sql(Box<SqlResource>),
}

impl CliProjectRunSource {
    pub(crate) fn as_project_resource(&self) -> ProjectRunSource<'_> {
        match self {
            Self::File(resource) => ProjectRunSource::file(resource.as_ref()),
            Self::Rest(resource) => ProjectRunSource::rest(resource.as_ref()),
            Self::Sql(resource) => ProjectRunSource::sql(resource.as_ref()),
        }
    }

    pub(crate) fn as_queryable(&self) -> &dyn QueryableResource {
        match self {
            Self::File(resource) => resource.as_ref(),
            Self::Rest(resource) => resource.as_ref(),
            Self::Sql(resource) => resource.as_ref(),
        }
    }
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
