use std::{collections::BTreeSet, fmt, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema};
use serde::{Deserialize, Serialize};

use crate::{
    BatchStats, CanonicalArrowSchema,
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

    /// Returns the ownership token already accounting for an in-memory payload.
    ///
    /// Invocation-local handoff stores may clone this token to retain the same accounted bytes
    /// across a compiler barrier; cloning does not reserve or double-count another buffer.
    pub fn retention(&self) -> Option<&PayloadRetention> {
        match &self.payload {
            BatchPayload::RecordBatch(payload) => payload.retention.as_ref(),
            BatchPayload::Reference(_) => None,
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
    pub observation_representation: PhysicalObservationRepresentation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_observation_schema: Option<CanonicalArrowSchema>,
    pub row_count: u64,
    pub byte_count: u64,
    pub source_position: Option<SourcePosition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pre_contract_quarantine: Vec<PreContractQuarantineFact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_coercion_plan: Option<String>,
    pub watermarks: Vec<crate::WatermarkClaim>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_idleness: Option<crate::PartitionIdlenessClaim>,
    pub stats: BatchStats,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cdc: Option<CdcMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cdc_settlement: Option<CdcSettlementMarker>,
    #[serde(skip, default)]
    pre_contract_evidence: PreContractEvidence,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalObservationRepresentation {
    ArrowSchema,
    MaterializedOutput,
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
            observation_representation: PhysicalObservationRepresentation::ArrowSchema,
            physical_observation_schema: None,
            row_count,
            byte_count,
            source_position: None,
            pre_contract_quarantine: Vec::new(),
            schema_coercion_plan: None,
            watermarks: Vec::new(),
            partition_idleness: None,
            stats: BatchStats::default(),
            cdc: None,
            cdc_settlement: None,
            pre_contract_evidence: PreContractEvidence::default(),
        }
    }

    pub fn mark_materialized_output(&mut self, physical_schema: &Schema) -> Result<()> {
        self.observation_representation = PhysicalObservationRepresentation::MaterializedOutput;
        self.observed_schema_hash = crate::canonical_arrow_schema_hash(physical_schema)?;
        self.physical_observation_schema = Some(CanonicalArrowSchema::from_arrow(physical_schema)?);
        Ok(())
    }

    pub fn materialized_physical_schema(&self) -> Result<Schema> {
        if self.observation_representation != PhysicalObservationRepresentation::MaterializedOutput
        {
            return Err(crate::CdfError::data(
                "physical schema observation is only available for materialized output",
            ));
        }
        let schema = self
            .physical_observation_schema
            .as_ref()
            .ok_or_else(|| {
                crate::CdfError::data(
                    "materialized output requires an exact typed physical schema observation",
                )
            })?
            .to_arrow()?;
        let actual = crate::canonical_arrow_schema_hash(&schema)?;
        if actual != self.observed_schema_hash {
            return Err(crate::CdfError::data(format!(
                "materialized typed schema hash {actual} does not match batch observation hash {}",
                self.observed_schema_hash
            )));
        }
        Ok(schema)
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

    pub fn extend_physical_reconciliations(
        &mut self,
        reconciliations: impl IntoIterator<Item = PreContractPhysicalReconciliation>,
    ) {
        self.pre_contract_evidence
            .physical_reconciliations
            .extend(reconciliations);
    }

    pub fn take_physical_reconciliations(&mut self) -> Vec<PreContractPhysicalReconciliation> {
        std::mem::take(&mut self.pre_contract_evidence.physical_reconciliations)
    }

    pub fn physical_reconciliations(&self) -> &[PreContractPhysicalReconciliation] {
        &self.pre_contract_evidence.physical_reconciliations
    }

    pub fn mark_materialized_residuals_complete(&mut self) {
        self.pre_contract_evidence.materialized_residuals_complete = true;
    }

    pub fn materialized_residuals_complete(&self) -> bool {
        self.pre_contract_evidence.materialized_residuals_complete
    }

    pub fn pre_contract_evidence_retained_bytes(&self) -> Result<u64> {
        let vector_bytes = self
            .pre_contract_evidence
            .residual_candidates
            .capacity()
            .checked_mul(std::mem::size_of::<PreContractResidualCandidate>())
            .and_then(|bytes| {
                self.pre_contract_evidence
                    .physical_reconciliations
                    .capacity()
                    .checked_mul(std::mem::size_of::<PreContractPhysicalReconciliation>())
                    .and_then(|physical| bytes.checked_add(physical))
            })
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| crate::CdfError::data("pre-contract evidence vector memory overflow"))?;
        let mut arrays = BTreeSet::new();
        let candidates = self
            .pre_contract_evidence
            .residual_candidates
            .iter()
            .try_fold(vector_bytes, |total, candidate| {
                let array_bytes = retained_array_bytes_once(&candidate.value, &mut arrays)?;
                total
                    .checked_add(candidate.owned_metadata_retained_bytes()?)
                    .and_then(|bytes| bytes.checked_add(array_bytes))
                    .ok_or_else(|| crate::CdfError::data("pre-contract evidence memory overflow"))
            })?;
        self.pre_contract_evidence
            .physical_reconciliations
            .iter()
            .try_fold(candidates, |total, reconciliation| {
                let array_bytes =
                    retained_array_bytes_once(&reconciliation.observed_values, &mut arrays)?;
                total
                    .checked_add(reconciliation.owned_metadata_retained_bytes()?)
                    .and_then(|bytes| bytes.checked_add(array_bytes))
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
    physical_reconciliations: Vec<PreContractPhysicalReconciliation>,
    materialized_residuals_complete: bool,
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

    pub fn retained_bytes(&self) -> Result<u64> {
        self.owned_metadata_retained_bytes()?
            .checked_add(array_retained_bytes(self.value.as_ref())?)
            .ok_or_else(|| crate::CdfError::data("pre-contract evidence memory overflow"))
    }

    fn owned_metadata_retained_bytes(&self) -> Result<u64> {
        pre_contract_metadata_bytes(
            &self.source_path,
            &self.observed_field,
            self.expected_field.as_ref(),
            0,
        )
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

#[derive(Clone)]
#[non_exhaustive]
pub struct PreContractPhysicalReconciliation {
    source_path: Vec<String>,
    observed_field: Field,
    expected_field: Field,
    observed_values: ArrayRef,
    batch_row_ordinals: Vec<usize>,
}

impl PreContractPhysicalReconciliation {
    pub fn new(
        source_path: Vec<String>,
        observed_field: Field,
        expected_field: Field,
        observed_values: ArrayRef,
        batch_row_ordinals: Vec<usize>,
    ) -> Result<Self> {
        if source_path.is_empty() || source_path.iter().any(String::is_empty) {
            return Err(crate::CdfError::data(
                "pre-contract physical reconciliation requires non-empty source path segments",
            ));
        }
        if observed_values.is_empty() || observed_values.len() != batch_row_ordinals.len() {
            return Err(crate::CdfError::data(
                "pre-contract physical reconciliation values must align with non-empty batch row ordinals",
            ));
        }
        if observed_values.data_type() != observed_field.data_type() {
            return Err(crate::CdfError::data(format!(
                "pre-contract physical reconciliation Arrow value type {} does not match observed field type {}",
                observed_values.data_type(),
                observed_field.data_type()
            )));
        }
        if batch_row_ordinals.windows(2).any(|rows| rows[0] >= rows[1]) {
            return Err(crate::CdfError::data(
                "pre-contract physical reconciliation batch rows must be strictly increasing",
            ));
        }
        Ok(Self {
            source_path,
            observed_field,
            expected_field,
            observed_values,
            batch_row_ordinals,
        })
    }

    pub fn source_path(&self) -> &[String] {
        &self.source_path
    }

    pub fn observed_field(&self) -> &Field {
        &self.observed_field
    }

    pub fn expected_field(&self) -> &Field {
        &self.expected_field
    }

    pub fn observed_values(&self) -> &ArrayRef {
        &self.observed_values
    }

    pub fn batch_row_ordinals(&self) -> &[usize] {
        &self.batch_row_ordinals
    }

    pub fn retained_bytes(&self) -> Result<u64> {
        self.owned_metadata_retained_bytes()?
            .checked_add(array_retained_bytes(self.observed_values.as_ref())?)
            .ok_or_else(|| crate::CdfError::data("pre-contract evidence memory overflow"))
    }

    fn owned_metadata_retained_bytes(&self) -> Result<u64> {
        let ordinal_bytes = self
            .batch_row_ordinals
            .capacity()
            .checked_mul(std::mem::size_of::<usize>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| {
                crate::CdfError::data("pre-contract physical reconciliation row memory exceeds u64")
            })?;
        pre_contract_metadata_bytes(
            &self.source_path,
            &self.observed_field,
            Some(&self.expected_field),
            ordinal_bytes,
        )
    }
}

fn retained_array_bytes_once(value: &ArrayRef, arrays: &mut BTreeSet<usize>) -> Result<u64> {
    let identity = Arc::as_ptr(value) as *const () as usize;
    if arrays.insert(identity) {
        array_retained_bytes(value.as_ref())
    } else {
        Ok(0)
    }
}

fn array_retained_bytes(value: &dyn Array) -> Result<u64> {
    u64::try_from(value.get_array_memory_size())
        .map_err(|_| crate::CdfError::data("pre-contract evidence array memory exceeds u64"))
}

fn pre_contract_metadata_bytes(
    source_path: &[String],
    observed_field: &Field,
    expected_field: Option<&Field>,
    initial: u64,
) -> Result<u64> {
    let path_bytes = source_path.iter().try_fold(0_u64, |total, segment| {
        total
            .checked_add(u64::try_from(segment.capacity()).unwrap_or(u64::MAX))
            .ok_or_else(|| crate::CdfError::data("pre-contract path memory overflow"))
    })?;
    let mut total = initial
        .checked_add(path_bytes)
        .and_then(|bytes| {
            u64::try_from(
                source_path
                    .len()
                    .saturating_mul(std::mem::size_of::<String>()),
            )
            .ok()
            .and_then(|path| bytes.checked_add(path))
        })
        .ok_or_else(|| crate::CdfError::data("pre-contract evidence memory overflow"))?;
    total = total
        .checked_add(field_retained_bytes(observed_field)?)
        .ok_or_else(|| crate::CdfError::data("pre-contract evidence memory overflow"))?;
    if let Some(expected) = expected_field {
        total = total
            .checked_add(field_retained_bytes(expected)?)
            .ok_or_else(|| crate::CdfError::data("pre-contract evidence memory overflow"))?;
    }
    Ok(total)
}

fn field_retained_bytes(field: &Field) -> Result<u64> {
    let mut bytes = std::mem::size_of::<Field>()
        .checked_add(field.name().len())
        .ok_or_else(|| crate::CdfError::data("pre-contract field memory overflow"))?;
    for (key, value) in field.metadata() {
        bytes = bytes
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<String>()))
            .ok_or_else(|| crate::CdfError::data("pre-contract field memory overflow"))?;
    }
    let nested = match field.data_type() {
        arrow_schema::DataType::List(child) => field_retained_bytes(child)?,
        arrow_schema::DataType::Struct(fields) => {
            fields.iter().try_fold(0_u64, |total, child| {
                total
                    .checked_add(field_retained_bytes(child)?)
                    .ok_or_else(|| crate::CdfError::data("pre-contract field memory overflow"))
            })?
        }
        _ => 0,
    };
    u64::try_from(bytes)
        .ok()
        .and_then(|bytes| bytes.checked_add(nested))
        .ok_or_else(|| crate::CdfError::data("pre-contract field memory overflow"))
}

impl fmt::Debug for PreContractPhysicalReconciliation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreContractPhysicalReconciliation")
            .field("source_path", &self.source_path)
            .field("observed_field", &self.observed_field)
            .field("expected_field", &self.expected_field)
            .field("value_type", self.observed_values.data_type())
            .field("row_count", &self.batch_row_ordinals.len())
            .finish()
    }
}

impl PartialEq for PreContractPhysicalReconciliation {
    fn eq(&self, other: &Self) -> bool {
        self.source_path == other.source_path
            && self.observed_field == other.observed_field
            && self.expected_field == other.expected_field
            && self.batch_row_ordinals == other.batch_row_ordinals
            && self.observed_values.to_data() == other.observed_values.to_data()
    }
}

impl Eq for PreContractPhysicalReconciliation {}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdcOperation {
    Insert,
    Update,
    Delete,
}

/// Source-proven settlement category carried by CDC control batches.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdcSettlementUnitKind {
    CommittedTransaction,
    EventPrefix,
}

/// Boundary represented by one zero-row CDC control batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdcSettlementBoundary {
    Begin,
    Terminal,
}

/// Explicit source boundary that prevents generic batch and timer cadence from manufacturing a
/// checkpoint inside a transaction or opaque ordered event prefix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CdcSettlementMarker {
    pub unit_kind: CdcSettlementUnitKind,
    pub boundary: CdcSettlementBoundary,
    pub position: SourcePosition,
}

impl CdcSettlementMarker {
    pub fn validate(&self) -> Result<()> {
        self.position.validate()?;
        let expected = match self.unit_kind {
            CdcSettlementUnitKind::CommittedTransaction => crate::SourcePositionKind::Log,
            CdcSettlementUnitKind::EventPrefix => crate::SourcePositionKind::ResumeToken,
        };
        if self.position.kind() != expected {
            return Err(crate::CdfError::data(format!(
                "CDC settlement marker kind requires a {} position, but received {}",
                expected.as_str(),
                self.position.kind().as_str()
            )));
        }
        self.position.cdc_protocol_order_identity().map(|_| ())
    }
}

/// Source-proven operation and ordering authority for one homogeneous CDC batch.
///
/// Adapters split native mixed-operation buffers at operation boundaries. This keeps key-only
/// deletes truthful without introducing nullable placeholder values for a complete after-image.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CdcMetadata {
    pub operation: CdcOperation,
    pub position: SourcePosition,
}

impl CdcMetadata {
    pub fn validate(&self, row_count: u64, batch_position: Option<&SourcePosition>) -> Result<()> {
        if row_count == 0 {
            return Err(crate::CdfError::data(
                "CDC operation metadata requires at least one row",
            ));
        }
        self.position.validate()?;
        self.position.cdc_protocol_order_identity()?;
        let batch_position = batch_position.ok_or_else(|| {
            crate::CdfError::data("CDC batches require exact source-position authority")
        })?;
        if !self.position.equivalent(batch_position)? {
            return Err(crate::CdfError::data(
                "CDC operation position does not match the batch source position",
            ));
        }
        Ok(())
    }
}
