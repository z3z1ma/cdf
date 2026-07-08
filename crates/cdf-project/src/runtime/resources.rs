use super::{prelude::*, validation::validate_local_file_run_resource};

#[derive(Clone, Copy)]
pub struct ProjectRunSource<'a> {
    resource: &'a dyn QueryableResource,
    validation: ProjectRunSourceValidation<'a>,
}

#[derive(Clone, Copy)]
enum ProjectRunSourceValidation<'a> {
    LocalFile(&'a CompiledResource),
    Rest(&'a RestResource),
    Sql(&'a SqlResource),
    Prevalidated,
}

impl std::fmt::Debug for ProjectRunSource<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectRunSource")
            .field("resource_id", &self.descriptor().resource_id)
            .field("state_scope", &self.descriptor().state_scope)
            .field("incremental", &self.capabilities().incremental)
            .finish_non_exhaustive()
    }
}

impl<'a> ProjectRunSource<'a> {
    pub fn new(resource: &'a dyn QueryableResource) -> Self {
        Self {
            resource,
            validation: ProjectRunSourceValidation::Prevalidated,
        }
    }

    pub fn local_file(resource: &'a CompiledResource) -> Self {
        Self {
            resource,
            validation: ProjectRunSourceValidation::LocalFile(resource),
        }
    }

    pub fn rest(resource: &'a RestResource) -> Self {
        Self {
            resource,
            validation: ProjectRunSourceValidation::Rest(resource),
        }
    }

    pub fn sql(resource: &'a SqlResource) -> Self {
        Self {
            resource,
            validation: ProjectRunSourceValidation::Sql(resource),
        }
    }

    pub fn stream(self) -> &'a dyn ResourceStream {
        self.resource
    }

    pub fn capabilities(self) -> &'a ResourceCapabilities {
        self.resource.capabilities()
    }

    pub fn descriptor(self) -> &'a ResourceDescriptor {
        self.resource.descriptor()
    }

    pub(super) fn validate_supported(self) -> Result<()> {
        match self.validation {
            ProjectRunSourceValidation::LocalFile(resource) => {
                validate_local_file_run_resource(resource)
            }
            ProjectRunSourceValidation::Rest(resource) => resource.validate_runtime_dependencies(),
            ProjectRunSourceValidation::Sql(resource) => resource.validate_runtime_dependencies(),
            ProjectRunSourceValidation::Prevalidated => Ok(()),
        }
    }
}
