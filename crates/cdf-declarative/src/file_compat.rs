use cdf_kernel::{CdfError, ResourceStream, Result};
use cdf_source_files::{FileResource, FileResourceDefinition, FileRuntimeDependencies};

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
    let (plan, compiled_format) =
        cdf_source_files::compile_file_resource_plan(plan, dependencies.formats())?;
    FileResource::new(
        FileResourceDefinition {
            descriptor: resource.descriptor().clone(),
            schema: resource.schema(),
            plan,
            type_policy_allowances: resource.type_policy_allowances(),
            effective_schema_runtime: resource.effective_schema_runtime().cloned(),
            compiled_format,
        },
        dependencies,
    )
}
