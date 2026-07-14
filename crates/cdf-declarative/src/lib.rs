#![doc = "Declarative resource authoring boundary for cdf."]

mod compiled;
mod declarations;
mod file_compat;
mod rest_compat;
mod sql_runtime;
#[cfg(test)]
mod tests;

pub use cdf_source_files::*;
pub use cdf_source_files::{
    FileCompressionDeclaration, FileFormatDeclaration, FileResource, FileResourcePlan,
    FileRuntimeDependencies, FileTransportFacade, LocalFileDiscoveryCandidate,
    local_file_discovery_candidates,
};
pub use cdf_source_postgres::{
    POSTGRES_CATALOG_DISCOVERY_PROBE, discover_postgres_table_catalog_schema,
};
pub use cdf_source_rest::{RestResource, RestResourcePlan, RestRuntimeDependencies};
pub use compiled::{
    CompiledResource, CompiledResourcePlan, SqlResourcePlan, compile_document,
    compile_document_with_project_root, parse_arrow_field_type, physical_arrow_schema_hash,
    validate_document,
};
pub use declarations::*;
pub use rest_compat::discover_rest_sample_schema;
pub use sql_runtime::{SqlResource, SqlRuntimeDependencies, postgres_table_target_for_sql_plan};
