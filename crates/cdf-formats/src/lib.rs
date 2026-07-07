#![doc = "External data format boundary for cdf."]

mod readers;
mod resource;
mod schema;
#[cfg(test)]
mod tests;
mod types;

pub use readers::{
    infer_ndjson_observed_schema, read_arrow_ipc_stream, read_csv_bytes, read_file_source,
    read_json_bytes, read_ndjson_bytes,
};
pub use resource::FileResource;
pub use schema::{SCHEMA_HASH_PREFIX, compile_observed_schema, schema_hash};
pub use types::*;
