use cdf_kernel::{CdfError, ResourceStream, Result};
use cdf_source_files::{FileResource, FileRuntimeDependencies};

use crate::{CompiledResource, CompiledResourcePlan};

impl CompiledResource {
    pub fn into_file_resource(self, dependencies: FileRuntimeDependencies) -> Result<FileResource> {
        build_file_resource(&self, dependencies)
    }

    pub fn to_file_resource(&self, dependencies: FileRuntimeDependencies) -> Result<FileResource> {
        build_file_resource(self, dependencies)
    }
}

fn build_file_resource(
    resource: &CompiledResource,
    dependencies: FileRuntimeDependencies,
) -> Result<FileResource> {
    let CompiledResourcePlan::Files(plan) = resource.plan() else {
        return Err(CdfError::contract(
            "only compiled file resources can be opened with file runtime dependencies",
        ));
    };
    FileResource::new(
        resource.descriptor().clone(),
        resource.schema(),
        resource.capabilities().clone(),
        plan.clone(),
        resource.type_policy_allowances(),
        resource.effective_schema_runtime().cloned(),
        dependencies,
    )
}
