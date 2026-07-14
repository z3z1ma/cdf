use cdf_kernel::{CdfError, ResourceStream, Result};
use cdf_runtime::CompiledFormatBinding;
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
    let compiled_format = CompiledFormatBinding::compile(
        dependencies.formats(),
        plan.format.as_str(),
        plan.format_options.clone(),
    )?;
    FileResource::new(
        FileResourceDefinition {
            descriptor: resource.descriptor().clone(),
            schema: resource.schema(),
            capabilities: resource.capabilities().clone(),
            plan: plan.clone(),
            type_policy_allowances: resource.type_policy_allowances(),
            effective_schema_runtime: resource.effective_schema_runtime().cloned(),
            compiled_format,
        },
        dependencies,
    )
}
