use std::{fmt, path::PathBuf};

use cdf_contract::ObservedSchema;
use cdf_kernel::{Batch, ResourceDescriptor, SchemaHash};
use cdf_runtime::ReadOptions;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    Csv(CsvOptions),
    Json(JsonOptions),
    Ndjson(JsonOptions),
    Parquet,
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
    pub options: ReadOptions,
}

impl FileSource {
    pub fn new(path: impl Into<PathBuf>, format: FileFormat, options: ReadOptions) -> Self {
        Self {
            path: path.into(),
            format,
            options,
        }
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
