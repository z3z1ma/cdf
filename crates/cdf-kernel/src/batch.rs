use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::{
    error::Result,
    ids::{BatchId, PartitionId, ResourceId, SchemaHash},
    position::SourcePosition,
};

#[derive(Clone, Debug)]
pub struct Batch {
    pub header: BatchHeader,
    pub payload: BatchPayload,
}

impl Batch {
    pub fn from_record_batch(
        batch_id: BatchId,
        resource_id: ResourceId,
        partition_id: PartitionId,
        observed_schema_hash: SchemaHash,
        record_batch: RecordBatch,
    ) -> Result<Self> {
        let row_count = record_batch.num_rows() as u64;
        let byte_count = record_batch.get_array_memory_size() as u64;
        Ok(Self {
            header: BatchHeader {
                batch_id,
                resource_id,
                partition_id,
                observed_schema_hash,
                row_count,
                byte_count,
                source_position: None,
                pre_contract_quarantine: Vec::new(),
                watermarks: Vec::new(),
                stats: BatchStats::default(),
                cdc: None,
            },
            payload: BatchPayload::RecordBatch(record_batch),
        })
    }

    pub fn from_reference(header: BatchHeader, reference: PayloadRef) -> Self {
        Self {
            header,
            payload: BatchPayload::Reference(reference),
        }
    }

    pub fn record_batch(&self) -> Option<&RecordBatch> {
        match &self.payload {
            BatchPayload::RecordBatch(record_batch) => Some(record_batch),
            BatchPayload::Reference(_) => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum BatchPayload {
    RecordBatch(RecordBatch),
    Reference(PayloadRef),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchHeader {
    pub batch_id: BatchId,
    pub resource_id: ResourceId,
    pub partition_id: PartitionId,
    pub observed_schema_hash: SchemaHash,
    pub row_count: u64,
    pub byte_count: u64,
    pub source_position: Option<SourcePosition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pre_contract_quarantine: Vec<PreContractQuarantineFact>,
    pub watermarks: Vec<Watermark>,
    pub stats: BatchStats,
    pub cdc: Option<CdcMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreContractQuarantineFact {
    pub source_row_ordinal: u64,
    pub rule_id: String,
    pub error_code: String,
    pub source_position: Option<SourcePosition>,
    pub observed_value_redacted: PreContractObservedValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PreContractObservedValue {
    Null,
    Preserved { value: String },
    Hashed { algorithm: String, value: String },
    Omitted,
    Masked { value: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadRef {
    pub uri: String,
    pub byte_count: u64,
    pub sha256: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchStats {
    pub columns: BTreeMap<String, ColumnStats>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnStats {
    pub null_count: Option<u64>,
    pub distinct_count: Option<u64>,
    pub min_lexical: Option<String>,
    pub max_lexical: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watermark {
    pub name: String,
    pub position: SourcePosition,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcMetadata {
    pub operation_field: String,
    pub position: SourcePosition,
}
