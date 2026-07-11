#![doc = "Declarative resource authoring boundary for cdf."]

mod compiled;
mod declarations;
mod file_compat;
mod rest_compat;
mod sql_runtime;
#[cfg(test)]
mod tests;

pub use cdf_formats::FileCompression;
pub use cdf_source_files::*;
pub use cdf_source_files::{
    FileCompressionDeclaration, FileFormatDeclaration, FileResource, FileResourcePlan,
    FileRuntimeDependencies, FileTransportFacade, LocalFileDiscoveryCandidate,
    local_file_discovery_candidates,
};
pub use cdf_source_rest::{RestResource, RestResourcePlan, RestRuntimeDependencies};
pub use compiled::{
    BoundedLocalParquetSchemaProbe, BoundedTransportJsonSchemaProbe, CompiledResource,
    CompiledResourcePlan, LocalArrowIpcSchemaProbe, LocalParquetSchemaProbe, SqlResourcePlan,
    compile_document, compile_document_with_project_root, discover_local_arrow_ipc_schema,
    discover_local_arrow_ipc_schema_bounded, discover_local_parquet_schema,
    discover_local_parquet_schema_bounded, discover_local_row_schema_bounded,
    discover_transport_parquet_schema, discover_transport_parquet_schema_bounded,
    discover_transport_row_schema_bounded, parse_arrow_field_type, physical_arrow_schema_hash,
    validate_document,
};
pub use declarations::*;
pub use rest_compat::discover_rest_sample_schema;
pub use sql_runtime::{SqlResource, SqlRuntimeDependencies, postgres_table_target_for_sql_plan};

#[cfg(test)]
pub(crate) fn test_execution_services() -> cdf_runtime::ExecutionServices {
    static SERVICES: std::sync::OnceLock<cdf_runtime::ExecutionServices> =
        std::sync::OnceLock::new();
    SERVICES
        .get_or_init(|| {
            cdf_engine::StandaloneExecutionHost::default_services(128 * 1024 * 1024)
                .expect("declarative test execution host")
                .1
        })
        .clone()
}
