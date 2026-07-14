#![doc = "External data format boundary for cdf."]

mod arrow_ipc_discovery;
mod readers;
mod resource;
#[cfg(test)]
mod tests;
mod types;

pub use arrow_ipc_discovery::{
    LocalArrowIpcSchemaDiscovery, LocalArrowIpcSourceIdentity, discover_arrow_ipc_file_schema,
    discover_local_arrow_ipc_schema, discover_local_arrow_ipc_schema_bounded,
};
pub use readers::{
    discover_csv_schema_from_reader, discover_json_schema_from_reader,
    discover_ndjson_schema_from_reader, infer_ndjson_observed_schema, read_arrow_ipc_file,
    read_arrow_ipc_file_path, read_arrow_ipc_file_path_with_declared_schema, read_arrow_ipc_stream,
    read_csv_bytes, read_file_source, read_file_source_with_declared_schema,
    read_file_source_with_declared_schema_and_type_policy, read_json_bytes, read_ndjson_bytes,
    read_ndjson_bytes_with_declared_schema, read_ndjson_bytes_with_declared_schema_and_type_policy,
    stream_file_source_path_with_declared_schema_and_type_policy,
    stream_parquet_file_with_declared_schema_and_type_policy,
};
pub use resource::FileResource;
pub use types::*;
