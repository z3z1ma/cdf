#![doc = "Package builder and reader boundary for cdf."]

mod archive;
mod artifacts;
mod builder;
mod json;
mod manifest_stream;
mod ops;
mod package_fs;
mod parquet;
mod quarantine;
mod reader;
mod runtime_schema;
mod statistics_profile;
mod storage;

pub use archive::*;
pub use builder::*;
pub use json::*;
pub use manifest_stream::*;
pub use ops::*;
pub use parquet::{transcode_record_batches_to_parquet_bytes, validate_parquet_schema};
pub use quarantine::*;
pub use reader::*;
pub use runtime_schema::*;
pub use statistics_profile::*;
pub use storage::encode_canonical_segment_ipc;

#[cfg(test)]
mod tests;
