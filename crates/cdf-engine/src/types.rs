use std::collections::BTreeMap;

use cdf_contract::{SchemaCoercionPlan, ValidationProgram};
use cdf_kernel::{
    BatchId, DeliveryGuarantee, EffectiveSchemaEvidence, EstimateSupport, PushdownFidelity,
    ResourceId, ScanPlan, ScanPredicate, ScanRequest, SchemaHash, SegmentId, SourcePosition,
    WriteDisposition,
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
    pub operator_chain: Vec<OperatorNode>,
    pub explain: ExplainData,
    pub package_id: String,
}

impl EnginePlan {
    pub fn effective_schema_evidence(&self) -> Option<&EffectiveSchemaPlanEvidence> {
        self.effective_schema_evidence.as_ref()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveSchemaPlanEvidence {
    pub authority: EffectiveSchemaEvidence,
    pub effective_arrow_schema_hash: SchemaHash,
    pub observations: Vec<EffectiveSchemaObservationCoercion>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineRunOutputWithSegmentPositions {
    pub output: EngineRunOutput,
    pub segment_positions: Vec<EngineSegmentPosition>,
}

#[derive(Clone, Copy, Debug)]
pub struct EnginePackageDraft<'a> {
    pub segments: &'a [SegmentEntry],
    pub profile: &'a ExecutionProfile,
    pub lineage: &'a LineageSummary,
    pub segment_positions: &'a [EngineSegmentPosition],
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
