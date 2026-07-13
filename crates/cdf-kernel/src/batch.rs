use std::{collections::BTreeSet, fmt, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::Field;
use serde::{Deserialize, Serialize};

use crate::{
    BatchStats,
    error::Result,
    ids::{BatchId, PartitionId, ResourceId, SchemaHash},
    position::SourcePosition,
    retention::PayloadRetention,
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
            header: BatchHeader::new(
                batch_id,
                resource_id,
                partition_id,
                observed_schema_hash,
                row_count,
                byte_count,
            ),
            payload: BatchPayload::in_memory(record_batch),
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
            BatchPayload::RecordBatch(payload) => Some(payload.batch()),
            BatchPayload::Reference(_) => None,
        }
    }

    pub fn with_retention(mut self, retention: PayloadRetention) -> Result<Self> {
        match &mut self.payload {
            BatchPayload::RecordBatch(payload) => payload.set_retention(retention),
            BatchPayload::Reference(_) => {
                return Err(crate::CdfError::contract(
                    "referenced batch payload cannot carry in-memory retention",
                ));
            }
        }
        Ok(self)
    }

    pub fn retained_bytes(&self) -> u64 {
        match &self.payload {
            BatchPayload::RecordBatch(payload) => payload.retained_bytes(),
            BatchPayload::Reference(_) => 0,
        }
    }
}

#[derive(Clone, Debug)]
pub enum BatchPayload {
    RecordBatch(RecordBatchPayload),
    Reference(PayloadRef),
}

impl BatchPayload {
    pub fn in_memory(batch: RecordBatch) -> Self {
        Self::RecordBatch(RecordBatchPayload {
            batch,
            retention: None,
        })
    }
}

#[derive(Clone, Debug)]
pub struct RecordBatchPayload {
    batch: RecordBatch,
    retention: Option<PayloadRetention>,
}

impl RecordBatchPayload {
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub fn retained_bytes(&self) -> u64 {
        self.retention.as_ref().map_or(0, PayloadRetention::bytes)
    }

    fn set_retention(&mut self, retention: PayloadRetention) {
        self.retention = Some(retention);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_coercion_plan: Option<String>,
    pub watermarks: Vec<Watermark>,
    pub stats: BatchStats,
    pub cdc: Option<CdcMetadata>,
    #[serde(skip, default)]
    pre_contract_evidence: PreContractEvidence,
}

impl BatchHeader {
    pub fn new(
        batch_id: BatchId,
        resource_id: ResourceId,
        partition_id: PartitionId,
        observed_schema_hash: SchemaHash,
        row_count: u64,
        byte_count: u64,
    ) -> Self {
        Self {
            batch_id,
            resource_id,
            partition_id,
            observed_schema_hash,
            row_count,
            byte_count,
            source_position: None,
            pre_contract_quarantine: Vec::new(),
            schema_coercion_plan: None,
            watermarks: Vec::new(),
            stats: BatchStats::default(),
            cdc: None,
            pre_contract_evidence: PreContractEvidence::default(),
        }
    }

    pub fn residual_candidates(&self) -> &[PreContractResidualCandidate] {
        &self.pre_contract_evidence.residual_candidates
    }

    pub fn push_residual_candidate(&mut self, candidate: PreContractResidualCandidate) {
        self.pre_contract_evidence
            .residual_candidates
            .push(candidate);
    }

    pub fn extend_residual_candidates(
        &mut self,
        candidates: impl IntoIterator<Item = PreContractResidualCandidate>,
    ) {
        self.pre_contract_evidence
            .residual_candidates
            .extend(candidates);
    }

    pub fn take_residual_candidates(&mut self) -> Vec<PreContractResidualCandidate> {
        std::mem::take(&mut self.pre_contract_evidence.residual_candidates)
    }

    pub fn pre_contract_evidence_retained_bytes(&self) -> Result<u64> {
        let mut seen = BTreeSet::new();
        self.pre_contract_evidence
            .residual_candidates
            .iter()
            .try_fold(0_u64, |total, candidate| {
                let allocation = Arc::as_ptr(&candidate.value) as *const () as usize;
                if !seen.insert(allocation) {
                    return Ok(total);
                }
                let bytes =
                    u64::try_from(candidate.value.get_array_memory_size()).map_err(|_| {
                        crate::CdfError::data("pre-contract evidence memory exceeds u64")
                    })?;
                total
                    .checked_add(bytes)
                    .ok_or_else(|| crate::CdfError::data("pre-contract evidence memory overflow"))
            })
    }

    pub fn set_payload_counts(&mut self, row_count: u64, byte_count: u64) {
        self.row_count = row_count;
        self.byte_count = byte_count;
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PreContractEvidence {
    residual_candidates: Vec<PreContractResidualCandidate>,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct PreContractResidualCandidate {
    source_row_ordinal: u64,
    batch_row_ordinal: usize,
    source_path: Vec<String>,
    observed_field: Field,
    expected_field: Option<Field>,
    value: ArrayRef,
    value_index: usize,
}

impl PreContractResidualCandidate {
    pub fn new(
        source_row_ordinal: u64,
        batch_row_ordinal: usize,
        source_path: Vec<String>,
        observed_field: Field,
        expected_field: Option<Field>,
        value: ArrayRef,
        value_index: usize,
    ) -> Result<Self> {
        if source_path.is_empty() || source_path.iter().any(String::is_empty) {
            return Err(crate::CdfError::data(
                "pre-contract residual candidate requires non-empty source path segments",
            ));
        }
        if value_index >= value.len() {
            return Err(crate::CdfError::data(format!(
                "pre-contract residual candidate value index {value_index} is outside array length {}",
                value.len()
            )));
        }
        if value.data_type() != observed_field.data_type() {
            return Err(crate::CdfError::data(format!(
                "pre-contract residual candidate Arrow value type {} does not match observed field type {}",
                value.data_type(),
                observed_field.data_type()
            )));
        }
        Ok(Self {
            source_row_ordinal,
            batch_row_ordinal,
            source_path,
            observed_field,
            expected_field,
            value,
            value_index,
        })
    }

    pub fn source_row_ordinal(&self) -> u64 {
        self.source_row_ordinal
    }

    pub fn batch_row_ordinal(&self) -> usize {
        self.batch_row_ordinal
    }

    pub fn source_path(&self) -> &[String] {
        &self.source_path
    }

    pub fn observed_field(&self) -> &Field {
        &self.observed_field
    }

    pub fn expected_field(&self) -> Option<&Field> {
        self.expected_field.as_ref()
    }

    pub fn value(&self) -> &dyn Array {
        self.value.as_ref()
    }

    pub fn value_index(&self) -> usize {
        self.value_index
    }
}

impl fmt::Debug for PreContractResidualCandidate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreContractResidualCandidate")
            .field("source_row_ordinal", &self.source_row_ordinal)
            .field("batch_row_ordinal", &self.batch_row_ordinal)
            .field("source_path", &self.source_path)
            .field("observed_field", &self.observed_field)
            .field("expected_field", &self.expected_field)
            .field("value_type", self.value.data_type())
            .field("value_is_null", &self.value.is_null(self.value_index))
            .finish()
    }
}

impl PartialEq for PreContractResidualCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.source_row_ordinal == other.source_row_ordinal
            && self.batch_row_ordinal == other.batch_row_ordinal
            && self.source_path == other.source_path
            && self.observed_field == other.observed_field
            && self.expected_field == other.expected_field
            && self.value_index == other.value_index
            && self.value.to_data() == other.value.to_data()
    }
}

impl Eq for PreContractResidualCandidate {}

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
