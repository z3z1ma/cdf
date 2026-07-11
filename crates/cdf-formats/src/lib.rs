#![doc = "External data format boundary for cdf."]

mod arrow_ipc_discovery;
mod parquet_discovery;
mod readers;
mod resource;
mod schema;
#[cfg(test)]
mod tests;
mod types;

pub use arrow_ipc_discovery::{
    LocalArrowIpcSchemaDiscovery, LocalArrowIpcSourceIdentity, discover_arrow_ipc_file_schema,
    discover_local_arrow_ipc_schema, discover_local_arrow_ipc_schema_bounded,
};
pub use parquet_discovery::{
    BoundedLocalParquetSchemaDiscovery, LocalParquetSchemaDiscovery, LocalParquetSourceIdentity,
    RangeChunkReader, discover_local_parquet_schema, discover_local_parquet_schema_bounded,
    discover_parquet_schema_from_chunk_reader,
};
pub use readers::{
    discover_csv_schema_from_reader, discover_json_schema_from_reader,
    discover_ndjson_schema_from_reader, infer_ndjson_observed_schema, read_arrow_ipc_file,
    read_arrow_ipc_file_path, read_arrow_ipc_file_path_with_declared_schema, read_arrow_ipc_stream,
    read_csv_bytes, read_file_source, read_file_source_with_declared_schema,
    read_file_source_with_declared_schema_and_type_policy, read_json_bytes, read_ndjson_bytes,
    read_ndjson_bytes_with_declared_schema, read_ndjson_bytes_with_declared_schema_and_type_policy,
    read_parquet_range_source, read_parquet_range_source_with_declared_schema,
    read_parquet_range_source_with_declared_schema_and_type_policy,
};
pub use resource::FileResource;
pub use schema::{SCHEMA_HASH_PREFIX, compile_observed_schema, schema_hash};
pub use types::*;
