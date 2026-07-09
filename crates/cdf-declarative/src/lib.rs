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
    CompiledResource, CompiledResourcePlan, FileResourcePlan, LocalParquetSchemaProbe,
    RestResourcePlan, SqlResourcePlan, compile_document, compile_document_with_project_root,
    discover_local_parquet_schema, validate_document,
};
pub use declarations::*;
pub use file_transport::*;
pub use rest_runtime::{
    RestResource, RestRuntimeDependencies, RestSampleSchemaDiscovery, discover_rest_sample_schema,
};
pub use sql_runtime::{SqlResource, SqlRuntimeDependencies, postgres_table_target_for_sql_plan};
