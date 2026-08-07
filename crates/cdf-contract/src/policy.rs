use arrow_schema::{DataType, Field};
use cdf_kernel::{CdfError, IdentifierRules, Result, TrustLevel, semantic};
pub use cdf_semantic::CDF_VARIANT_SEMANTIC;
use serde::{Deserialize, Serialize};

use crate::schema::ArrowType;

pub const NORMALIZER_NAMECASE_V1: &str = "namecase-v1";
pub const VARIANT_COLUMN_NAME: &str = "_cdf_variant";
pub fn is_framework_variant_field(field: &Field) -> bool {
    field.name() == VARIANT_COLUMN_NAME
        && field.data_type() == &DataType::Utf8
        && field.is_nullable()
        && semantic(field) == Some(CDF_VARIANT_SEMANTIC)
        && field
            .metadata()
            .get(crate::RESIDUAL_ENCODING_METADATA_KEY)
            .is_some_and(|encoding| encoding == crate::RESIDUAL_ENCODING_NAME)
}

const NORMALIZER_POSTGRES_QUOTED_V1: &str = "namecase-v1/postgres-quoted-v1";
const NORMALIZER_SQLITE_QUOTED_V1: &str = "namecase-v1/sqlite-quoted-v1";
const DUCKDB_NAMECASE_ALLOWED_PATTERN: &str = "^[a-z_][a-z0-9_]*$";
const POSTGRES_QUOTED_ALLOWED_PATTERN: &str =
    "quoted UTF-8 identifier without NUL; cdf reserves _cdf_*";
const SQLITE_QUOTED_ALLOWED_PATTERN: &str =
    "quoted UTF-8 identifier without NUL; cdf reserves _cdf_*";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractPolicy {
    pub schema: SchemaPolicy,
    pub types: TypePolicy,
    pub rows: RowPolicy,
    pub admission: AdmissionPolicy,
    pub evidence: EvidencePolicy,
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
    fn base(admission: AdmissionPolicy) -> Self {
        Self {
            schema: SchemaPolicy::default(),
            types: TypePolicy::strict_fidelity(),
            rows: RowPolicy::full(),
            admission,
            evidence: EvidencePolicy::default(),
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

    pub fn for_trust(trust: TrustLevel) -> Self {
        match trust {
            TrustLevel::Experimental => Self::experimental(),
            TrustLevel::Governed => Self::governed(),
            TrustLevel::Financial => Self::financial(),
            TrustLevel::Serving => Self::serving(),
        }
    }

    fn experimental() -> Self {
        let mut policy = Self::base(AdmissionPolicy::experimental());
        policy.normalization.nested =
            NestedDataPolicy::VariantCapture(VariantColumnSpec::default());
        policy.profiling = ProfilingPolicy::Sampled;
        policy.retention = RetentionClass::Ephemeral;
        policy
    }

    fn governed() -> Self {
        let mut policy = Self::base(AdmissionPolicy::governed());
        policy.schema.review_artifact_required = true;
        policy.rows.validation_depth = ValidationDepth::Full;
        policy.normalization.nested =
            NestedDataPolicy::VariantCapture(VariantColumnSpec::default());
        policy.retention = RetentionClass::PackageRetained;
        policy
    }

    fn financial() -> Self {
        let mut policy = Self::base(AdmissionPolicy::financial());
        policy.types = TypePolicy::strict_fidelity();
        policy.rows.validation_depth = ValidationDepth::Full;
        policy.lineage = LineagePolicy::Full;
        policy.receipts_required = true;
        policy.reconciliation_counts = true;
        policy.retention = RetentionClass::Long;
        policy
    }

    fn serving() -> Self {
        let mut policy = Self::base(AdmissionPolicy::governed());
        policy.rows.validation_depth = ValidationDepth::SampledFastPath {
            clean_runs_required: policy.promotion.clean_runs_required,
        };
        policy.rows.freshness_slo = true;
        policy.promotion.allow_sampled_fast_path = true;
        policy.promotion.demote_on_anomaly = true;
        policy.normalization.nested =
            NestedDataPolicy::VariantCapture(VariantColumnSpec::default());
        policy
    }
}

impl Default for ContractPolicy {
    fn default() -> Self {
        Self::governed()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPolicy {
    pub review_artifact_required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionPolicy {
    pub field: FieldDisposition,
    pub row: RowViolationDisposition,
    pub record: RecordViolationDisposition,
    pub partition: PartitionViolationDisposition,
}

impl AdmissionPolicy {
    pub fn experimental() -> Self {
        Self {
            field: FieldDisposition::CaptureVariant,
            row: RowViolationDisposition::FailRun,
            record: RecordViolationDisposition::FailRun,
            partition: PartitionViolationDisposition::FailRun,
        }
    }

    pub fn governed() -> Self {
        Self {
            field: FieldDisposition::CaptureVariant,
            row: RowViolationDisposition::QuarantineRow,
            record: RecordViolationDisposition::QuarantineRecord,
            partition: PartitionViolationDisposition::QuarantinePartition,
        }
    }

    pub fn financial() -> Self {
        Self {
            field: FieldDisposition::FailRun,
            row: RowViolationDisposition::FailRun,
            record: RecordViolationDisposition::FailRun,
            partition: PartitionViolationDisposition::FailRun,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldDisposition {
    CaptureVariant,
    QuarantineRow,
    FailRun,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowViolationDisposition {
    QuarantineRow,
    FailRun,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordViolationDisposition {
    QuarantineRecord,
    FailRun,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionViolationDisposition {
    QuarantinePartition,
    FailRun,
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidencePolicy {
    pub pii_redaction: PiiRedactionPolicy,
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
        NORMALIZER_SQLITE_QUOTED_V1 => match rules.allowed_pattern.as_deref() {
            None | Some(SQLITE_QUOTED_ALLOWED_PATTERN) => Ok(None),
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
}

impl Default for VariantColumnSpec {
    fn default() -> Self {
        Self {
            column_name: VARIANT_COLUMN_NAME.to_owned(),
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
        expression: crate::DeclarativeExpression,
    },
    Filter {
        expression: crate::DeclarativeExpression,
    },
    ExpandNested {
        column: String,
        policy: NestedDataPolicy,
    },
}
