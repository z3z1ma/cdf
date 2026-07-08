use super::{prelude::*, validation::validate_local_file_run_resource};

#[derive(Clone, Copy, Debug)]
pub enum ProjectRunResource<'a> {
    LocalFile(&'a CompiledResource),
    Rest(&'a RestResource),
    Sql(&'a SqlResource),
}

impl<'a> ProjectRunResource<'a> {
    pub fn local_file(resource: &'a CompiledResource) -> Self {
        Self::LocalFile(resource)
    }

    pub fn rest(resource: &'a RestResource) -> Self {
        Self::Rest(resource)
    }

    pub fn sql(resource: &'a SqlResource) -> Self {
        Self::Sql(resource)
    }

    pub(super) fn stream(self) -> &'a dyn ResourceStream {
        match self {
            Self::LocalFile(resource) => resource,
            Self::Rest(resource) => resource,
            Self::Sql(resource) => resource,
        }
    }

    pub(super) fn descriptor(self) -> &'a ResourceDescriptor {
        self.stream().descriptor()
    }

    pub(super) fn validate_supported(self) -> Result<()> {
        match self {
            Self::LocalFile(resource) => validate_local_file_run_resource(resource),
            Self::Rest(resource) => resource.validate_runtime_dependencies(),
            Self::Sql(resource) => resource.validate_runtime_dependencies(),
        }
    }
}
