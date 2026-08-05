use std::{collections::BTreeMap, fs, path::Path, sync::Arc};

use cdf_declarative::CompiledResource;
use cdf_http::{SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{
    CdfError, DestinationProtocol, QueryableResource, Result, SchemaSource, WriteDisposition,
};
use cdf_object_access::FileTransportFacade;
use cdf_runtime::{
    ByteTransformRegistry, CompiledSourcePlan, FormatRegistry, SourceRegistry,
    SourceResolutionContext,
};
use cdf_source_files::{FileRuntimeDependencies, FileSourceDriver, file_source_blocking_lane};

struct NoSecrets;

impl SecretProvider for NoSecrets {
    fn resolve(&self, uri: &SecretUri) -> Result<SecretValue> {
        Err(CdfError::auth(format!(
            "conformance fixture has no secret for {uri}"
        )))
    }
}

pub(crate) struct ResolvedSourceFixture {
    resource: Arc<dyn QueryableResource>,
    source_plan: CompiledSourcePlan,
    #[cfg(test)]
    execution: cdf_runtime::ExecutionServices,
}

impl ResolvedSourceFixture {
    pub(crate) fn resolve(
        compiled: &CompiledResource,
        registry: &SourceRegistry,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Self> {
        Ok(Self {
            resource: registry.resolve(compiled.source_plan(), context)?,
            source_plan: compiled.source_plan().clone(),
            #[cfg(test)]
            execution: context.execution().clone(),
        })
    }

    pub(crate) fn queryable(&self) -> &dyn QueryableResource {
        self.resource.as_ref()
    }

    pub(crate) fn bind_plan(&self, plan: cdf_engine::EnginePlan) -> Result<cdf_engine::EnginePlan> {
        plan.bind_compiled_source(&self.source_plan)
    }

    #[cfg(test)]
    pub(crate) fn execution(&self) -> &cdf_runtime::ExecutionServices {
        &self.execution
    }
}

pub(crate) fn resolve_local_file(
    resource: &CompiledResource,
    project_root: &Path,
) -> Result<ResolvedSourceFixture> {
    let execution = crate::test_execution_services();
    let registry = local_file_registry()?;
    let context = SourceResolutionContext::new(
        project_root,
        Arc::new(NoSecrets),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    ResolvedSourceFixture::resolve(resource, &registry, &context)
}

#[cfg(test)]
pub(crate) fn resolve_with_registry(
    resource: &CompiledResource,
    registry: &SourceRegistry,
    project_root: &Path,
    driver_options: BTreeMap<String, serde_json::Value>,
) -> Result<ResolvedSourceFixture> {
    let execution = crate::test_execution_services();
    let context = SourceResolutionContext::new(
        project_root,
        Arc::new(NoSecrets),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_driver_options(driver_options);
    ResolvedSourceFixture::resolve(resource, registry, &context)
}

pub(crate) fn local_file_registry() -> Result<SourceRegistry> {
    let mut formats = FormatRegistry::default();
    formats.register(Arc::new(cdf_format_json::NdjsonFormatDriver::new()?))?;
    let formats = Arc::new(formats);
    let mut registry = SourceRegistry::new();
    let compile_formats = Arc::clone(&formats);
    registry.register(FileSourceDriver::new(
        compile_formats,
        move |secrets, execution, egress| {
            Ok(FileRuntimeDependencies::new(
                FileTransportFacade::new()
                    .with_shared_secret_provider(secrets)
                    .with_execution_services(execution.clone())
                    .with_local_listing_lane(file_source_blocking_lane())?,
                execution,
                formats.clone(),
                Arc::new(ByteTransformRegistry::default()),
                egress,
            ))
        },
    )?)?;
    Ok(registry)
}

pub(crate) fn compile_local_file_project_resource(
    project_root: &Path,
    project_name: &str,
    glob: &str,
    disposition: WriteDisposition,
    input_schema: &arrow_schema::Schema,
) -> Result<CompiledResource> {
    let resource_directory = project_root.join("cdf/local");
    fs::create_dir_all(&resource_directory).map_err(|error| {
        crate::conformance_private_io_error("create query-first resource directory", error)
    })?;
    let project_toml = format!(
        r#"[project]
name = "{project_name}"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.sqlite"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[sources.local]
type = "files"
root = "data"
"#
    );
    fs::write(project_root.join("cdf.toml"), &project_toml).map_err(|error| {
        crate::conformance_private_io_error("write query-first project configuration", error)
    })?;
    let disposition = match disposition {
        WriteDisposition::Append => "APPEND",
        WriteDisposition::Replace => "REPLACE",
        WriteDisposition::Merge => "MERGE(id)",
        WriteDisposition::CdcApply => {
            return Err(CdfError::contract(
                "local file conformance fixture does not support CDC apply",
            ));
        }
    };
    let sql = format!(
        "RESOURCE\nDISPOSITION {disposition}\nTRUST GOVERNED\nEXECUTION BOUNDED\nAS\nSELECT * FROM upstream(source => 'local', glob => '{glob}', format => 'ndjson');\n"
    );
    fs::write(resource_directory.join("events.cdf.sql"), sql).map_err(|error| {
        crate::conformance_private_io_error("write query-first resource SQL", error)
    })?;

    let config = cdf_project::parse_cdf_toml(&project_toml)?;
    let registry = local_file_registry()?;
    let destination =
        cdf_dest_duckdb::DuckDbDestination::new(project_root.join(".cdf/compile-only.duckdb"))?;
    let schema_hash = cdf_kernel::canonical_arrow_schema_hash(input_schema)?;
    let input_schemas = BTreeMap::from([(
        "local.events".to_owned(),
        cdf_project::ProjectInputSchemaAuthority::new(
            SchemaSource::Declared {
                schema_hash,
                source: "conformance-fixture".to_owned(),
            },
            input_schema.clone(),
        )?,
    )]);
    let mut resources = cdf_project::compile_query_project_resources(
        &registry,
        &config,
        project_root,
        "dev",
        destination.sheet(),
        &cdf_semantic::SemanticCatalog::builtins()?,
        &input_schemas,
    )?;
    if resources.len() != 1 {
        return Err(CdfError::contract(format!(
            "file conformance fixture expected one resource, found {}",
            resources.len()
        )));
    }
    Ok(resources.remove(0).resource)
}
