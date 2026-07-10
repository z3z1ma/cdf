use std::{collections::BTreeMap, sync::Arc};

use arrow_schema::Schema;
use cdf_contract::{CanonicalArrowField, SchemaCoercionPlan, ValidationProgram};
use cdf_kernel::{
    BatchId, CdfError, DeliveryGuarantee, DiscoveryExecutorBudgetEvidence, EffectiveSchemaEvidence,
    EstimateSupport, ProcessedObservationPosition, PushdownFidelity, ResourceId, Result, ScanPlan,
    ScanPredicate, ScanRequest, SchemaHash, SegmentId, SourcePosition,
    TerminalSchemaObservationQuarantine, WriteDisposition,
};
use cdf_package::{PackageManifest, SegmentEntry};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnginePlanInput {
    pub request: ScanRequest,
    pub validation_program: ValidationProgram,
    pub boundedness: PlanBoundedness,
    pub package_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PlanBoundedness {
    Bounded,
    UnboundedDrain,
    UnboundedLive {
        checkpoint_cadence_ms: Option<u64>,
        package_rotation_rows: Option<u64>,
        watermark: Option<String>,
    },
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnginePlan {
    pub scan: ScanPlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_schema_evidence: Option<EffectiveSchemaPlanEvidence>,
    pub final_projection: Option<Vec<String>>,
    pub residual_predicates: Vec<ScanPredicate>,
    pub boundedness: PlanBoundedness,
    #[serde(default = "default_write_disposition")]
    pub write_disposition: WriteDisposition,
    pub validation_program: ValidationProgram,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_authority: Option<EngineSchemaAuthority>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<EngineOutputSchema>,
    pub operator_chain: Vec<OperatorNode>,
    pub explain: ExplainData,
    pub package_id: String,
}

impl EnginePlan {
    pub fn effective_schema_evidence(&self) -> Option<&EffectiveSchemaPlanEvidence> {
        self.effective_schema_evidence.as_ref()
    }

    pub fn output_arrow_schema(&self) -> Result<Arc<Schema>> {
        self.output_schema
            .as_ref()
            .ok_or_else(|| CdfError::data("engine plan has no compiled output schema"))?
            .to_arrow()
    }

    pub fn schema_authority(&self) -> Result<&EngineSchemaAuthority> {
        self.schema_authority
            .as_ref()
            .ok_or_else(|| CdfError::data("engine plan has no verified schema authority"))
    }

    pub fn effective_schema_hash(&self) -> Result<&SchemaHash> {
        Ok(&self.schema_authority()?.effective_schema_hash)
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineSchemaAuthority {
    pub version: u16,
    pub baseline_schema_hash: SchemaHash,
    pub effective_schema_hash: SchemaHash,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineOutputSchema {
    pub version: u16,
    pub arrow_schema_hash: SchemaHash,
    pub fields: Vec<CanonicalArrowField>,
    pub metadata: BTreeMap<String, String>,
}

impl EngineOutputSchema {
    pub fn from_arrow(schema: &Schema) -> Result<Self> {
        let arrow_schema_hash = cdf_contract::canonical_arrow_schema_hash(schema)?;
        let fields = schema
            .fields()
            .iter()
            .map(|field| CanonicalArrowField::from_arrow(field))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            version: 1,
            arrow_schema_hash,
            fields,
            metadata: schema
                .metadata()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        })
    }

    pub fn to_arrow(&self) -> Result<Arc<Schema>> {
        if self.version != 1 {
            return Err(CdfError::data(format!(
                "unsupported engine output schema version {}",
                self.version
            )));
        }
        let fields = self
            .fields
            .iter()
            .map(CanonicalArrowField::to_arrow)
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            self.metadata.clone().into_iter().collect(),
        ));
        let actual = cdf_contract::canonical_arrow_schema_hash(schema.as_ref())?;
        if actual != self.arrow_schema_hash {
            return Err(CdfError::data(format!(
                "compiled output schema hash mismatch: plan records {}, materialized {}",
                self.arrow_schema_hash, actual
            )));
        }
        Ok(schema)
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveSchemaPlanEvidence {
    pub authority: EffectiveSchemaEvidence,
    pub effective_arrow_schema_hash: SchemaHash,
    pub observations: Vec<EffectiveSchemaObservationCoercion>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub terminal_quarantines: Vec<TerminalSchemaObservationQuarantine>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_executor_budget: Option<DiscoveryExecutorBudgetEvidence>,
    pub observation_bindings: BTreeMap<String, String>,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveSchemaObservationCoercion {
    pub observation_id: String,
    pub physical_schema_hash: SchemaHash,
    pub coercion_plan: SchemaCoercionPlan,
}

fn default_write_disposition() -> WriteDisposition {
    WriteDisposition::Append
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OperatorNode {
    CdfResourceAdapter {
        adapter_kind: String,
        resource_id: ResourceId,
    },
    CdfNativeScan {
        projection: Option<Vec<String>>,
        residual_predicates: Vec<String>,
        limit: Option<u64>,
    },
    SchemaFingerprintExec,
    ContractExec {
        normalizer_version: String,
        column_program_count: usize,
    },
    NormalizeExec {
        normalizer_version: String,
    },
    ProfileExec,
    LineageExec,
    PackageSink {
        package_id: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplainData {
    pub resource_id: ResourceId,
    pub projected_fields: Vec<String>,
    pub projection_pushed: bool,
    pub limit: Option<u64>,
    pub limit_pushed: bool,
    pub pushed_predicates: Vec<PredicateExplain>,
    pub inexact_predicates: Vec<PredicateExplain>,
    pub unsupported_predicates: Vec<PredicateExplain>,
    pub partitions: Vec<PartitionExplain>,
    pub estimates: EstimateExplain,
    pub delivery_guarantee: DeliveryGuarantee,
    pub boundedness: PlanBoundedness,
    pub operator_chain: Vec<OperatorNode>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredicateExplain {
    pub predicate_id: String,
    pub expression: String,
    pub fidelity: PushdownFidelity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionExplain {
    pub partition_id: String,
    pub scope_kind: String,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EstimateExplain {
    pub support: EstimateSupport,
    pub rows: Option<u64>,
    pub bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineRunOutput {
    pub manifest: PackageManifest,
    pub segments: Vec<SegmentEntry>,
    pub profile: ExecutionProfile,
    pub lineage: LineageSummary,
}

pub const ENGINE_EXECUTION_EVIDENCE_VERSION: u16 = 1;

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineExecutionEvidence {
    version: u16,
    processed_observations: Vec<ProcessedObservationPosition>,
}

impl Default for EngineExecutionEvidence {
    fn default() -> Self {
        Self {
            version: ENGINE_EXECUTION_EVIDENCE_VERSION,
            processed_observations: Vec::new(),
        }
    }
}

impl EngineExecutionEvidence {
    pub fn new(
        mut processed_observations: Vec<ProcessedObservationPosition>,
    ) -> cdf_kernel::Result<Self> {
        processed_observations
            .sort_by(|left, right| left.observation_id.cmp(&right.observation_id));
        let mut coalesced = Vec::<ProcessedObservationPosition>::new();
        for observation in processed_observations {
            match coalesced.last() {
                Some(existing)
                    if existing.observation_id == observation.observation_id
                        && existing != &observation =>
                {
                    return Err(cdf_kernel::CdfError::data(format!(
                        "repeated processed observation {:?} produced conflicting outcome or position evidence",
                        observation.observation_id
                    )));
                }
                Some(existing) if existing == &observation => {}
                _ => coalesced.push(observation),
            }
        }
        Ok(Self {
            version: ENGINE_EXECUTION_EVIDENCE_VERSION,
            processed_observations: coalesced,
        })
    }

    pub fn version(&self) -> u16 {
        self.version
    }

    pub fn processed_observations(&self) -> &[ProcessedObservationPosition] {
        &self.processed_observations
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineRunOutputWithSegmentPositions {
    pub output: EngineRunOutput,
    pub segment_positions: Vec<EngineSegmentPosition>,
    pub(crate) execution_evidence: EngineExecutionEvidence,
}

impl EngineRunOutputWithSegmentPositions {
    pub fn new(output: EngineRunOutput, segment_positions: Vec<EngineSegmentPosition>) -> Self {
        Self {
            output,
            segment_positions,
            execution_evidence: EngineExecutionEvidence::default(),
        }
    }

    pub fn execution_evidence(&self) -> &EngineExecutionEvidence {
        &self.execution_evidence
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug)]
pub struct EnginePackageDraft<'a> {
    pub segments: &'a [SegmentEntry],
    pub profile: &'a ExecutionProfile,
    pub lineage: &'a LineageSummary,
    pub segment_positions: &'a [EngineSegmentPosition],
    pub(crate) execution_evidence: &'a EngineExecutionEvidence,
}

impl<'a> EnginePackageDraft<'a> {
    pub fn execution_evidence(&self) -> &'a EngineExecutionEvidence {
        self.execution_evidence
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineSegmentPosition {
    pub segment_id: SegmentId,
    pub output_position: Option<SourcePosition>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionProfile {
    pub output_rows: u64,
    pub output_bytes: u64,
    pub output_batches: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageSummary {
    pub input_batches: Vec<BatchId>,
    pub output_segments: Vec<SegmentId>,
}
