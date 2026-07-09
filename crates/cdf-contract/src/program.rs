use cdf_kernel::{BatchId, ResourceId, SchemaHash};
use serde::{Deserialize, Serialize};

use crate::{
    policy::{
        IdentifierPolicy, PromotionPolicy, RedactionDecision, TransformDescription,
        ValidationDepth, VerdictAction,
    },
    reconciliation::SchemaCoercionPlan,
    schema::ArrowType,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationProgram {
    pub normalizer_version: String,
    #[serde(default)]
    pub identifier_policy: IdentifierPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_coercion: Option<SchemaCoercionPlan>,
    pub schema_verdicts: Vec<SchemaVerdictRule>,
    pub column_programs: Vec<ColumnProgram>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub row_rules: Vec<RowRuleProgram>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub explicit_anomalies: Vec<AnomalyFact>,
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

    pub fn requires_observed_at_ms(&self) -> bool {
        self.row_rules.iter().any(|rule| {
            matches!(
                &rule.predicate,
                RowRulePredicate::Freshness {
                    column: _,
                    max_age_ms: _
                }
            )
        })
    }

    pub fn has_dedup_rule(&self) -> bool {
        self.row_rules
            .iter()
            .any(|rule| matches!(rule.predicate, RowRulePredicate::Dedup { .. }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnomalyFact {
    pub metric: String,
    pub observed: String,
    pub threshold: String,
    pub window: String,
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
pub struct RowRuleProgram {
    pub rule_id: String,
    pub predicate: RowRulePredicate,
    #[serde(default, skip_serializing_if = "MissingColumnBehavior::is_error")]
    pub missing_column: MissingColumnBehavior,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RowRulePredicate {
    Nullability {
        column: String,
    },
    Domain {
        column: String,
        allowed: Vec<String>,
    },
    Range {
        column: String,
        min: Option<String>,
        max: Option<String>,
    },
    Regex {
        column: String,
        pattern: String,
    },
    Freshness {
        column: String,
        max_age_ms: u64,
    },
    Dedup {
        keys: Vec<String>,
        keep: DedupKeepProgram,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DedupKeepProgram {
    First,
    Last,
    Fail,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingColumnBehavior {
    #[default]
    Error,
    Skip,
}

impl MissingColumnBehavior {
    fn is_error(&self) -> bool {
        matches!(self, Self::Error)
    }
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
