use firn_kernel::{BatchId, ResourceId, SchemaHash};
use serde::{Deserialize, Serialize};

use crate::{
    policy::{
        PromotionPolicy, RedactionDecision, TransformDescription, ValidationDepth, VerdictAction,
    },
    schema::ArrowType,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationProgram {
    pub normalizer_version: String,
    pub schema_verdicts: Vec<SchemaVerdictRule>,
    pub column_programs: Vec<ColumnProgram>,
    pub row_dispositions: Vec<RowDispositionRule>,
    pub transforms: Vec<TransformDescription>,
    pub promotion: PromotionPolicy,
    pub warnings: Vec<CompileWarning>,
}

impl ValidationProgram {
    pub fn disposition_for(
        &self,
        outcome: RuleOutcome,
        rule_id: impl Into<String>,
    ) -> RuleDisposition {
        let rule_id = rule_id.into();
        let action = self
            .row_dispositions
            .iter()
            .find(|rule| rule.outcome == outcome)
            .map(|rule| &rule.disposition)
            .unwrap_or(&RowDispositionKind::RejectRun);

        match action {
            RowDispositionKind::Accept => RuleDisposition::Accept,
            RowDispositionKind::Quarantine => RuleDisposition::Quarantine { rule_id },
            RowDispositionKind::RejectBatch => RuleDisposition::RejectBatch { rule_id },
            RowDispositionKind::RejectRun => RuleDisposition::RejectRun { rule_id },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaVerdictRule {
    pub change: SchemaChangeKind,
    pub verdict: VerdictAction,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaChangeKind {
    NewTable,
    NewColumn,
    TypeWidening,
    TypeNarrowing,
    UnknownField,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnProgram {
    pub source_name: String,
    pub output_name: String,
    pub arrow_type: ArrowType,
    pub steps: Vec<ColumnProgramStep>,
    pub nested_action: NestedAction,
    pub redaction: RedactionDecision,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ColumnProgramStep {
    PreserveDecimalExactness,
    PreserveTimestampTimezone,
    ApplyTransform(TransformDescription),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NestedAction {
    NotNested,
    KeepNested,
    ExpandToChildTable {
        child_table: String,
    },
    CaptureVariant {
        column_name: String,
        semantic: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowDispositionRule {
    pub outcome: RuleOutcome,
    pub disposition: RowDispositionKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleOutcome {
    Pass,
    Coerced,
    AdmittedAsVariant,
    Violation,
    Fatal,
}

impl RuleOutcome {
    pub const ALL: [Self; 5] = [
        Self::Pass,
        Self::Coerced,
        Self::AdmittedAsVariant,
        Self::Violation,
        Self::Fatal,
    ];
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowDispositionKind {
    Accept,
    Quarantine,
    RejectBatch,
    RejectRun,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RuleDisposition {
    Accept,
    Quarantine { rule_id: String },
    RejectBatch { rule_id: String },
    RejectRun { rule_id: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompileWarning {
    pub rule_id: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationDepthTransitionEvent {
    pub resource_id: ResourceId,
    pub from_depth: ValidationDepth,
    pub to_depth: ValidationDepth,
    pub trigger: ValidationTransitionTrigger,
    pub schema_hash: Option<SchemaHash>,
    pub batch_id: Option<BatchId>,
    pub occurred_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ValidationTransitionTrigger {
    NewResource,
    CleanStableRuns { count: u32 },
    Drift,
    AnomalySpike,
    QuarantineEvent,
    Manual,
}
