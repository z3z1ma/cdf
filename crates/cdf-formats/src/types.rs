use std::{fmt, path::PathBuf};

use cdf_contract::ObservedSchema;
use cdf_kernel::{
    Batch, CdfError, PartitionId, ResourceDescriptor, ResourceId, Result, SchemaHash,
};
use serde::{Deserialize, Serialize};

pub const DEFAULT_BATCH_SIZE: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadOptions {
    pub resource_id: ResourceId,
    pub partition_id: PartitionId,
    pub batch_id_prefix: String,
    pub batch_size: usize,
}

impl ReadOptions {
    pub fn new(resource_id: ResourceId, partition_id: PartitionId) -> Self {
        let batch_id_prefix = format!(
            "{}-{}",
            sanitize_id_part(resource_id.as_str()),
            sanitize_id_part(partition_id.as_str())
        );
        Self {
            resource_id,
            partition_id,
            batch_id_prefix,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn with_batch_id_prefix(mut self, prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        if prefix.trim().is_empty() {
            return Err(CdfError::contract("batch id prefix cannot be empty"));
        }
        self.batch_id_prefix = prefix;
        Ok(self)
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Result<Self> {
        if batch_size == 0 {
            return Err(CdfError::contract("batch size must be greater than zero"));
        }
        self.batch_size = batch_size;
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    Csv(CsvOptions),
    Json(JsonOptions),
    Ndjson(JsonOptions),
    Parquet,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileCompression {
    None,
    Gzip,
    Zstd,
}

impl FileCompression {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Zstd => "zstd",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvOptions {
    pub has_header: bool,
    pub delimiter: u8,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonOptions {
    pub max_read_records: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileSource {
    pub path: PathBuf,
    pub format: FileFormat,
    pub compression: FileCompression,
    pub options: ReadOptions,
}

impl FileSource {
    pub fn new(path: impl Into<PathBuf>, format: FileFormat, options: ReadOptions) -> Self {
        Self {
            path: path.into(),
            format,
            compression: FileCompression::None,
            options,
        }
    }

    pub fn with_compression(mut self, compression: FileCompression) -> Self {
        self.compression = compression;
        self
    }
}

pub struct FormatRead {
    pub descriptor: ResourceDescriptor,
    pub observed_schema: ObservedSchema,
    pub schema_hash: SchemaHash,
    pub batches: Vec<Batch>,
}

impl Clone for FormatRead {
    fn clone(&self) -> Self {
        Self {
            descriptor: self.descriptor.clone(),
            observed_schema: ObservedSchema {
                fields: self.observed_schema.fields.clone(),
            },
            schema_hash: self.schema_hash.clone(),
            batches: self.batches.clone(),
        }
    }
}

impl fmt::Debug for FormatRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FormatRead")
            .field("descriptor", &self.descriptor)
            .field("observed_schema", &self.observed_schema.fields)
            .field("schema_hash", &self.schema_hash)
            .field("batches", &self.batches)
            .finish()
    }
}

fn sanitize_id_part(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect()
}
