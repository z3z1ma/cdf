use cdf_kernel::{BatchId, ResourceId, SchemaHash};
use serde::{Deserialize, Serialize};

use crate::{
    expression::{Expression, ExpressionLiteral, ExpressionNode, FunctionReference},
    policy::{
        IdentifierPolicy, PiiRedactionPolicy, PromotionPolicy, RedactionDecision,
        TransformDescription, ValidationDepth, VerdictAction,
    },
    reconciliation::SchemaCoercionPlan,
    schema::ArrowType,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ValidationProgram {
    /// Compilers leave this empty; the engine fills it after schema-aware analysis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled_expression_plan: Option<crate::CompiledExpressionPlan>,
    pub normalizer_version: String,
    pub identifier_policy: IdentifierPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_coercion: Option<SchemaCoercionPlan>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residual: Option<ResidualProgram>,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ResidualProgram {
    pub default_verdict: ResidualCandidateVerdict,
    pub pii_redaction: PiiRedactionPolicy,
    pub fields: Vec<ResidualFieldProgram>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture: Option<ResidualCaptureOutput>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ResidualFieldProgram {
    pub source_name: String,
    pub output_name: String,
    pub required: bool,
    pub control_critical: bool,
    pub redaction: RedactionDecision,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ResidualCaptureOutput {
    pub variant_column: String,
    pub semantic: String,
    pub encoding: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ResidualCandidateVerdict {
    Capture,
    Quarantine,
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
        self.row_rules
            .iter()
            .any(|rule| rule.expression_function() == Some("fresh_within"))
    }

    pub fn has_dedup_rule(&self) -> bool {
        self.has_keyed_dedup_rule() || self.has_exact_row_dedup_rule()
    }

    pub fn has_keyed_dedup_rule(&self) -> bool {
        self.row_rules
            .iter()
            .any(|rule| rule.expression_function() == Some("dedup"))
    }

    pub fn has_exact_row_dedup_rule(&self) -> bool {
        self.row_rules
            .iter()
            .any(|rule| rule.expression_function() == Some("exact_row_dedup"))
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
    pub expression: Expression,
    #[serde(default, skip_serializing_if = "MissingColumnBehavior::is_error")]
    pub missing_column: MissingColumnBehavior,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum NativeRowRule<'a> {
    Nullability {
        column: &'a str,
    },
    Domain {
        column: &'a str,
        allowed: &'a [String],
    },
    Range {
        column: &'a str,
        min: Option<&'a str>,
        max: Option<&'a str>,
    },
    Regex {
        column: &'a str,
        pattern: &'a str,
    },
    Freshness {
        column: &'a str,
        max_age_ms: u64,
    },
    Dedup {
        keys: &'a [String],
        keep: DedupKeepProgram,
    },
    ExactRowDedup {
        keys: &'a [String],
        keep: DedupKeepProgram,
    },
}

impl RowRuleProgram {
    pub fn expression_function(&self) -> Option<&str> {
        match &self.expression.root {
            ExpressionNode::Call { function, .. } => Some(function.name.as_str()),
            _ => None,
        }
    }

    pub fn is_dedup_expression(&self) -> bool {
        matches!(
            self.expression_function(),
            Some("dedup" | "exact_row_dedup")
        )
    }

    pub(crate) fn native_rule(&self) -> cdf_kernel::Result<NativeRowRule<'_>> {
        self.expression.validate()?;
        let ExpressionNode::Call {
            function,
            arguments,
        } = &self.expression.root
        else {
            return Err(cdf_kernel::CdfError::contract(format!(
                "contract rule {:?} must be a function expression",
                self.rule_id
            )));
        };
        if function != &FunctionReference::cdf(function.name.clone()) {
            return Err(cdf_kernel::CdfError::contract(format!(
                "contract rule {:?} uses unsupported function {}.{}@{}",
                self.rule_id, function.namespace, function.name, function.version
            )));
        }
        match (function.name.as_str(), arguments.as_slice()) {
            ("is_not_null", [ExpressionNode::Column { name }]) => {
                Ok(NativeRowRule::Nullability { column: name })
            }
            (
                "in_domain",
                [
                    ExpressionNode::Column { name },
                    ExpressionNode::Literal {
                        value: ExpressionLiteral::StringList(allowed),
                    },
                ],
            ) => Ok(NativeRowRule::Domain {
                column: name,
                allowed,
            }),
            (
                "in_range",
                [
                    ExpressionNode::Column { name },
                    ExpressionNode::Literal { value: min },
                    ExpressionNode::Literal { value: max },
                ],
            ) => Ok(NativeRowRule::Range {
                column: name,
                min: optional_string(min)?,
                max: optional_string(max)?,
            }),
            (
                "matches_regex",
                [
                    ExpressionNode::Column { name },
                    ExpressionNode::Literal {
                        value: ExpressionLiteral::String(pattern),
                    },
                ],
            ) => Ok(NativeRowRule::Regex {
                column: name,
                pattern,
            }),
            (
                "fresh_within",
                [
                    ExpressionNode::Column { name },
                    ExpressionNode::Literal {
                        value: ExpressionLiteral::Unsigned(max_age_ms),
                    },
                ],
            ) => Ok(NativeRowRule::Freshness {
                column: name,
                max_age_ms: *max_age_ms,
            }),
            (
                "dedup" | "exact_row_dedup",
                [
                    ExpressionNode::Literal {
                        value: ExpressionLiteral::StringList(keys),
                    },
                    ExpressionNode::Literal {
                        value: ExpressionLiteral::String(keep),
                    },
                ],
            ) => {
                let keep = match keep.as_str() {
                    "first" => DedupKeepProgram::First,
                    "last" => DedupKeepProgram::Last,
                    "fail" => DedupKeepProgram::Fail,
                    _ => {
                        return Err(cdf_kernel::CdfError::contract(format!(
                            "contract rule {:?} has unsupported dedup keep mode {keep:?}",
                            self.rule_id
                        )));
                    }
                };
                if function.name == "dedup" {
                    Ok(NativeRowRule::Dedup { keys, keep })
                } else {
                    Ok(NativeRowRule::ExactRowDedup { keys, keep })
                }
            }
            _ => Err(cdf_kernel::CdfError::contract(format!(
                "contract rule {:?} expression has no admitted native lowering",
                self.rule_id
            ))),
        }
    }

    pub(crate) fn referenced_columns(&self) -> Vec<&str> {
        fn visit<'a>(node: &'a ExpressionNode, output: &mut Vec<&'a str>) {
            match node {
                ExpressionNode::Column { name } => output.push(name.as_str()),
                ExpressionNode::Call { arguments, .. } => {
                    for argument in arguments {
                        visit(argument, output);
                    }
                }
                _ => {}
            }
        }
        let mut columns = Vec::new();
        visit(&self.expression.root, &mut columns);
        columns
    }
}

fn optional_string(value: &ExpressionLiteral) -> cdf_kernel::Result<Option<&str>> {
    match value {
        ExpressionLiteral::Null => Ok(None),
        ExpressionLiteral::String(value) => Ok(Some(value)),
        _ => Err(cdf_kernel::CdfError::contract(
            "contract range bounds must be string or null literals",
        )),
    }
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
