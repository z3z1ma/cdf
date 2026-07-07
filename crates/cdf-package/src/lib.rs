#![doc = "Package builder and reader boundary for cdf."]

mod archive;
mod artifacts;
mod builder;
mod json;
mod model;
mod ops;
mod parquet;
mod reader;
mod storage;

pub use archive::*;
pub use artifacts::*;
pub use builder::*;
pub use json::*;
pub use model::*;
pub use ops::*;
pub use parquet::transcode_record_batches_to_parquet_bytes;
pub use reader::*;

#[cfg(test)]
mod tests;
