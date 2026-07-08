use cdf_declarative::{
    CompiledResource, CompiledResourcePlan, RestResource, RestRuntimeDependencies, SqlResource,
    SqlRuntimeDependencies,
};
use cdf_project::ProjectRunSource;

use crate::{context::ProjectContext, http_transport::ReqwestHttpTransport, output::CliError};

pub(crate) enum CliProjectRunSource<'a> {
    LocalFile(&'a CompiledResource),
    Rest(Box<RestResource>),
    Sql(Box<SqlResource>),
}

impl<'a> CliProjectRunSource<'a> {
    pub(crate) fn as_project_resource(&'a self) -> ProjectRunSource<'a> {
        match self {
            Self::LocalFile(resource) => ProjectRunSource::local_file(resource),
            Self::Rest(resource) => ProjectRunSource::rest(resource.as_ref()),
            Self::Sql(resource) => ProjectRunSource::sql(resource.as_ref()),
        }
    }
}

pub(crate) fn build_project_run_resource<'a>(
    context: &ProjectContext,
    resource: &'a CompiledResource,
) -> Result<CliProjectRunSource<'a>, CliError> {
    match resource.plan() {
        CompiledResourcePlan::Files(_) => Ok(CliProjectRunSource::LocalFile(resource)),
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
