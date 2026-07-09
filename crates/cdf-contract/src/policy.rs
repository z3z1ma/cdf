use cdf_kernel::{CdfError, IdentifierRules, Result, TrustLevel};
use serde::{Deserialize, Serialize};

use crate::schema::ArrowType;

pub const NORMALIZER_NAMECASE_V1: &str = "namecase-v1";
pub const VARIANT_COLUMN_NAME: &str = "_cdf_variant";
pub const VARIANT_SEMANTIC_TAG: &str = "json";

const NORMALIZER_POSTGRES_QUOTED_V1: &str = "namecase-v1/postgres-quoted-v1";
const DUCKDB_NAMECASE_ALLOWED_PATTERN: &str = "^[a-z_][a-z0-9_]*$";
const POSTGRES_QUOTED_ALLOWED_PATTERN: &str =
    "quoted UTF-8 identifier without NUL; cdf reserves _cdf_*";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractPolicy {
    pub schema: SchemaPolicy,
    pub types: TypePolicy,
    pub rows: RowPolicy,
    pub verdicts: VerdictPolicy,
    pub quarantine: QuarantinePolicy,
    pub normalization: NormalizationPolicy,
    pub profiling: ProfilingPolicy,
    pub lineage: LineagePolicy,
    pub receipts_required: bool,
    pub reconciliation_counts: bool,
    pub retention: RetentionClass,
    pub promotion: PromotionPolicy,
    pub transforms: Vec<TransformDescription>,
}

impl ContractPolicy {
    pub fn evolve() -> Self {
        Self {
            schema: SchemaPolicy::evolve(),
            types: TypePolicy::strict_fidelity(),
            rows: RowPolicy::full(),
            verdicts: VerdictPolicy::quarantine_on_violation(),
            quarantine: QuarantinePolicy::enabled(),
            normalization: NormalizationPolicy::default(),
            profiling: ProfilingPolicy::Sampled,
            lineage: LineagePolicy::Package,
            receipts_required: false,
            reconciliation_counts: false,
            retention: RetentionClass::PackageRetained,
            promotion: PromotionPolicy::default(),
            transforms: Vec::new(),
        }
    }

    pub fn freeze() -> Self {
        Self {
            schema: SchemaPolicy::freeze(),
            ..Self::evolve()
        }
    }

    pub fn for_trust(trust: TrustLevel) -> Self {
        match trust {
            TrustLevel::Experimental => Self::experimental(),
            TrustLevel::Governed => Self::governed(),
            TrustLevel::Financial => Self::financial(),
            TrustLevel::Serving => Self::serving(),
        }
    }

    fn experimental() -> Self {
        let mut policy = Self::evolve();
        policy.quarantine = QuarantinePolicy::disabled();
        policy.normalization.nested =
            NestedDataPolicy::VariantCapture(VariantColumnSpec::default());
        policy.profiling = ProfilingPolicy::Sampled;
        policy.retention = RetentionClass::Ephemeral;
        policy.verdicts = VerdictPolicy::fail_on_violation();
        policy
    }

    fn governed() -> Self {
        let mut policy = Self::evolve();
        policy.schema.review_artifact_required = true;
        policy.rows.validation_depth = ValidationDepth::Full;
        policy.quarantine = QuarantinePolicy::enabled();
        policy.retention = RetentionClass::PackageRetained;
        policy
    }

    fn financial() -> Self {
        let mut policy = Self::freeze();
        policy.types = TypePolicy::strict_fidelity();
        policy.rows.validation_depth = ValidationDepth::Full;
        policy.lineage = LineagePolicy::Full;
        policy.receipts_required = true;
        policy.reconciliation_counts = true;
        policy.retention = RetentionClass::Long;
        policy.quarantine = QuarantinePolicy::enabled();
        policy
    }

    fn serving() -> Self {
        let mut policy = Self::freeze();
        policy.rows.validation_depth = ValidationDepth::SampledFastPath {
            clean_runs_required: policy.promotion.clean_runs_required,
        };
        policy.rows.freshness_slo = true;
        policy.promotion.allow_sampled_fast_path = true;
        policy.promotion.demote_on_anomaly = true;
        policy.quarantine = QuarantinePolicy::enabled();
        policy
    }
}

impl Default for ContractPolicy {
    fn default() -> Self {
        Self::evolve()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPolicy {
    pub mode: SchemaEvolutionMode,
    pub allow_new_table: bool,
    pub allow_new_column: bool,
    pub allow_type_widening: bool,
    pub quarantine_type_narrowing: bool,
    pub allow_unknown_fields: bool,
    pub review_artifact_required: bool,
}

impl SchemaPolicy {
    pub fn evolve() -> Self {
        Self {
            mode: SchemaEvolutionMode::Evolve,
            allow_new_table: true,
            allow_new_column: true,
            allow_type_widening: true,
            quarantine_type_narrowing: true,
            allow_unknown_fields: true,
            review_artifact_required: false,
        }
    }

    pub fn freeze() -> Self {
        Self {
            mode: SchemaEvolutionMode::Freeze,
            allow_new_table: false,
            allow_new_column: false,
            allow_type_widening: false,
            quarantine_type_narrowing: true,
            allow_unknown_fields: false,
            review_artifact_required: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaEvolutionMode {
    Evolve,
    Freeze,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypePolicy {
    pub coerce_types: bool,
    pub preserve_decimal_exactness: bool,
    pub preserve_timestamp_timezone: bool,
    pub allow_lossy_mapping: bool,
}

impl TypePolicy {
    pub fn strict_fidelity() -> Self {
        Self {
            coerce_types: true,
            preserve_decimal_exactness: true,
            preserve_timestamp_timezone: true,
            allow_lossy_mapping: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowPolicy {
    pub validation_depth: ValidationDepth,
    pub freshness_slo: bool,
    pub rules: Vec<RowRule>,
}

impl RowPolicy {
    pub fn full() -> Self {
        Self {
            validation_depth: ValidationDepth::Full,
            freshness_slo: false,
            rules: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ValidationDepth {
    Discovery,
    Full,
    Sampled,
    SampledFastPath { clean_runs_required: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RowRule {
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
        keep: DedupKeep,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DedupKeep {
    First,
    Last,
    Fail,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerdictPolicy {
    pub violation: VerdictAction,
    pub fatal: VerdictAction,
}

impl VerdictPolicy {
    pub fn quarantine_on_violation() -> Self {
        Self {
            violation: VerdictAction::Quarantine,
            fatal: VerdictAction::RejectRun,
        }
    }

    pub fn fail_on_violation() -> Self {
        Self {
            violation: VerdictAction::RejectBatch,
            fatal: VerdictAction::RejectRun,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerdictAction {
    Admit,
    AdmitAsVariant,
    Quarantine,
    RejectBatch,
    RejectRun,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuarantinePolicy {
    pub enabled: bool,
    pub pii_redaction: PiiRedactionPolicy,
}

impl QuarantinePolicy {
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            pii_redaction: PiiRedactionPolicy::default(),
        }
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            pii_redaction: PiiRedactionPolicy::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PiiRedactionPolicy {
    pub pii_action: RedactionDecision,
    pub default_action: RedactionDecision,
}

impl Default for PiiRedactionPolicy {
    fn default() -> Self {
        Self {
            pii_action: RedactionDecision::Hash {
                algorithm: "sha256".to_owned(),
            },
            default_action: RedactionDecision::Preserve,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RedactionDecision {
    Preserve,
    Hash { algorithm: String },
    Omit,
    Mask { replacement: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizationPolicy {
    pub identifier: IdentifierPolicy,
    pub nested: NestedDataPolicy,
}

impl Default for NormalizationPolicy {
    fn default() -> Self {
        Self {
            identifier: IdentifierPolicy::default(),
            nested: NestedDataPolicy::KeepNested,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentifierPolicy {
    pub version: String,
    #[serde(default = "default_identifier_max_length")]
    pub max_length: Option<u16>,
    pub charset: IdentifierCharset,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_pattern: Option<String>,
}

impl Default for IdentifierPolicy {
    fn default() -> Self {
        Self {
            version: NORMALIZER_NAMECASE_V1.to_owned(),
            max_length: Some(63),
            charset: IdentifierCharset::AsciiLowerSnake,
            allowed_pattern: None,
        }
    }
}

fn default_identifier_max_length() -> Option<u16> {
    Some(63)
}

impl IdentifierPolicy {
    pub fn from_destination_rules(rules: &IdentifierRules) -> Result<Self> {
        let policy = Self {
            max_length: rules.max_length,
            allowed_pattern: destination_allowed_pattern(rules)?,
            ..Self::default()
        };

        if let Some(max_length) = policy.max_length
            && max_length < 10
        {
            return Err(CdfError::contract(format!(
                "destination identifier rule {:?} max_length {} must leave room for hash suffix",
                rules.normalizer, max_length
            )));
        }

        Ok(policy)
    }
}

impl TryFrom<&IdentifierRules> for IdentifierPolicy {
    type Error = CdfError;

    fn try_from(rules: &IdentifierRules) -> std::result::Result<Self, Self::Error> {
        Self::from_destination_rules(rules)
    }
}

pub fn identifier_policy_from_destination_rules(
    rules: &IdentifierRules,
) -> Result<IdentifierPolicy> {
    IdentifierPolicy::from_destination_rules(rules)
}

fn destination_allowed_pattern(rules: &IdentifierRules) -> Result<Option<String>> {
    match rules.normalizer.as_str() {
        NORMALIZER_NAMECASE_V1 => match rules.allowed_pattern.as_deref() {
            None => Ok(None),
            Some(DUCKDB_NAMECASE_ALLOWED_PATTERN) => Ok(rules.allowed_pattern.clone()),
            Some(pattern) => Err(destination_rule_adapter_error(
                rules.normalizer.as_str(),
                Some(pattern),
            )),
        },
        NORMALIZER_POSTGRES_QUOTED_V1 => match rules.allowed_pattern.as_deref() {
            None | Some(POSTGRES_QUOTED_ALLOWED_PATTERN) => Ok(None),
            Some(pattern) => Err(destination_rule_adapter_error(
                rules.normalizer.as_str(),
                Some(pattern),
            )),
        },
        rule => Err(destination_rule_adapter_error(rule, None)),
    }
}

fn destination_rule_adapter_error(rule: &str, allowed_pattern: Option<&str>) -> CdfError {
    let pattern_context = allowed_pattern
        .map(|pattern| format!(" with allowed_pattern {pattern:?}"))
        .unwrap_or_default();
    CdfError::contract(format!(
        "destination identifier rule {rule:?}{pattern_context}: live column normalization for that rule is not implemented by this adapter"
    ))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentifierCharset {
    AsciiLowerSnake,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NestedDataPolicy {
    KeepNested,
    ChildTableExpansion {
        parent_keys: Vec<String>,
        load_order_column: String,
    },
    VariantCapture(VariantColumnSpec),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariantColumnSpec {
    pub column_name: String,
    pub semantic: String,
}

impl Default for VariantColumnSpec {
    fn default() -> Self {
        Self {
            column_name: VARIANT_COLUMN_NAME.to_owned(),
            semantic: VARIANT_SEMANTIC_TAG.to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfilingPolicy {
    Off,
    Sampled,
    Full,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineagePolicy {
    Package,
    Full,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionClass {
    Ephemeral,
    PackageRetained,
    Long,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotionPolicy {
    pub clean_runs_required: u32,
    pub allow_sampled_fast_path: bool,
    pub demote_on_drift: bool,
    pub demote_on_anomaly: bool,
    pub demote_on_quarantine: bool,
}

impl Default for PromotionPolicy {
    fn default() -> Self {
        Self {
            clean_runs_required: 3,
            allow_sampled_fast_path: false,
            demote_on_drift: true,
            demote_on_anomaly: true,
            demote_on_quarantine: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TransformDescription {
    Rename {
        from: String,
        to: String,
    },
    Cast {
        column: String,
        to: ArrowType,
        lossy_allowed: bool,
    },
    Redact {
        column: String,
        decision: RedactionDecision,
    },
    Derive {
        column: String,
        expression: String,
    },
    Filter {
        expression: String,
    },
    ExpandNested {
        column: String,
        policy: NestedDataPolicy,
    },
}
