#![doc = "Declarative resource authoring boundary for cdf."]

mod compiled;
mod declarations;
mod file_runtime;
mod file_transport;
mod rest_runtime;
mod sql_runtime;
#[cfg(test)]
mod tests;

pub use compiled::{
    BoundedLocalParquetSchemaProbe, CompiledResource, CompiledResourcePlan, FileResourcePlan,
    LocalArrowIpcSchemaProbe, LocalParquetSchemaProbe, RestResourcePlan, SqlResourcePlan,
    compile_document, compile_document_with_project_root, discover_local_arrow_ipc_schema,
    discover_local_arrow_ipc_schema_bounded, discover_local_parquet_schema,
    discover_local_parquet_schema_bounded, discover_transport_parquet_schema,
    physical_arrow_schema_hash, validate_document,
};
pub use declarations::*;
pub use file_runtime::{
    FileResource, FileRuntimeDependencies, LocalFileDiscoveryCandidate,
    local_file_discovery_candidates,
};
pub use file_transport::*;
pub use rest_runtime::{
    RestResource, RestRuntimeDependencies, RestSampleSchemaDiscovery, discover_rest_sample_schema,
};
pub use sql_runtime::{SqlResource, SqlRuntimeDependencies, postgres_table_target_for_sql_plan};
