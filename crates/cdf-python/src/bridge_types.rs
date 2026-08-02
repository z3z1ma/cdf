use cdf_kernel::{CdfError, PartitionId, ResourceDescriptor, ResourceId, Result, SchemaHash};
use serde::{Deserialize, Serialize};

pub const ARROW_C_ARRAY_METHOD: &str = "__arrow_c_array__";
pub const ARROW_C_STREAM_METHOD: &str = "__arrow_c_stream__";
pub const DEFAULT_DICT_BATCH_ROWS: usize = 8 * 1024;
pub const DEFAULT_MAX_BOUNDARY_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PythonBridgeOptions {
    pub resource_id: ResourceId,
    pub partition_id: PartitionId,
    pub batch_id_prefix: String,
    pub dict_batch_rows: usize,
    pub max_boundary_bytes: u64,
}

impl PythonBridgeOptions {
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
            dict_batch_rows: DEFAULT_DICT_BATCH_ROWS,
            max_boundary_bytes: DEFAULT_MAX_BOUNDARY_BYTES,
        }
    }

    pub fn with_dict_batch_rows(mut self, dict_batch_rows: usize) -> Result<Self> {
        if dict_batch_rows == 0 {
            return Err(CdfError::contract(
                "dict batch rows must be greater than zero",
            ));
        }
        self.dict_batch_rows = dict_batch_rows;
        Ok(self)
    }

    pub fn with_max_boundary_bytes(mut self, max_boundary_bytes: u64) -> Result<Self> {
        if max_boundary_bytes < 2 {
            return Err(CdfError::contract(
                "Python boundary byte limit must be at least 2 bytes",
            ));
        }
        self.max_boundary_bytes = max_boundary_bytes;
        Ok(self)
    }

    pub fn with_resource_id(mut self, resource_id: ResourceId) -> Self {
        self.resource_id = resource_id;
        self.batch_id_prefix = format!(
            "{}-{}",
            sanitize_id_part(self.resource_id.as_str()),
            sanitize_id_part(self.partition_id.as_str())
        );
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PythonYieldKind {
    DictRows,
    ArrowCArray,
    ArrowCStream,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrowCapsuleBoundary {
    pub kind: PythonYieldKind,
    pub method: String,
    pub capsule_names: Vec<String>,
    pub zero_copy_intent: bool,
}

impl ArrowCapsuleBoundary {
    pub fn for_c_array() -> Self {
        Self {
            kind: PythonYieldKind::ArrowCArray,
            method: ARROW_C_ARRAY_METHOD.to_owned(),
            capsule_names: vec!["arrow_schema".to_owned(), "arrow_array".to_owned()],
            zero_copy_intent: true,
        }
    }

    pub fn for_c_stream() -> Self {
        Self {
            kind: PythonYieldKind::ArrowCStream,
            method: ARROW_C_STREAM_METHOD.to_owned(),
            capsule_names: vec!["arrow_array_stream".to_owned()],
            zero_copy_intent: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PythonFirstObservation {
    pub descriptor: ResourceDescriptor,
    pub schema_hash: SchemaHash,
}

#[derive(Clone, Debug, Default)]
pub struct PythonStreamSummary {
    pub first_observation: Option<PythonFirstObservation>,
    pub outcome_count: u64,
    pub row_count: u64,
    pub byte_count: u64,
    pub peak_boundary_bytes: u64,
    pub dict_row_outcomes: u64,
    pub arrow_c_array_outcomes: u64,
    pub arrow_c_stream_outcomes: u64,
}

pub(crate) fn sanitize_id_part(value: &str) -> String {
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
