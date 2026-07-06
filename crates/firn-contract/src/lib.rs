#![doc = "Schema contract compilation boundary for firn."]

use std::collections::{BTreeMap, BTreeSet};

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use firn_kernel::{
    BatchId, FirnError, ResourceId, Result, SchemaHash, TrustLevel, TypeMapping,
    TypeMappingFidelity, semantic, source_name, with_source_name,
};
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

pub const NORMALIZER_NAMECASE_V1: &str = "namecase-v1";
pub const VARIANT_COLUMN_NAME: &str = "_firn_variant";
pub const VARIANT_SEMANTIC_TAG: &str = "json";

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
    pub max_length: u16,
    pub charset: IdentifierCharset,
}

impl Default for IdentifierPolicy {
    fn default() -> Self {
        Self {
            version: NORMALIZER_NAMECASE_V1.to_owned(),
            max_length: 63,
            charset: IdentifierCharset::AsciiLowerSnake,
        }
    }
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedSchema {
    pub fields: Vec<ObservedField>,
}

impl ObservedSchema {
    pub fn from_arrow(schema: &Schema) -> Self {
        Self::from_arrow_with_claims(schema, BTreeMap::new())
    }

    pub fn from_arrow_with_claims(
        schema: &Schema,
        source_claims: BTreeMap<String, SourceTypeClaim>,
    ) -> Self {
        let fields = schema
            .fields()
            .iter()
            .map(|field_ref| {
                let field = field_ref.as_ref();
                let source = source_name(field)
                    .unwrap_or_else(|| field.name())
                    .to_owned();
                let metadata = field
                    .metadata()
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
                let source_type = source_claims.get(&source).cloned();

                ObservedField {
                    name: field.name().clone(),
                    source_name: source,
                    arrow_type: ArrowType::from(field.data_type()),
                    nullable: field.is_nullable(),
                    metadata,
                    source_type,
                }
            })
            .collect();

        Self { fields }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedField {
    pub name: String,
    pub source_name: String,
    pub arrow_type: ArrowType,
    pub nullable: bool,
    pub metadata: BTreeMap<String, String>,
    pub source_type: Option<SourceTypeClaim>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceTypeClaim {
    Decimal { precision: u8, scale: i8 },
    Timestamp { timezone: TimestampZoneClaim },
    Other { name: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimestampZoneClaim {
    Zoned { zone: String },
    Naive,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArrowType {
    Null,
    Boolean,
    Int {
        signed: bool,
        bits: u8,
    },
    Float {
        bits: u8,
    },
    Decimal {
        bits: u16,
        precision: u8,
        scale: i8,
    },
    Timestamp {
        unit: TimeUnitName,
        timezone: Option<String>,
    },
    Utf8,
    Binary,
    Struct,
    List,
    Map,
    Other {
        display: String,
    },
}

impl ArrowType {
    fn is_nested(&self) -> bool {
        matches!(self, Self::Struct | Self::List | Self::Map)
    }

    fn is_float(&self) -> bool {
        matches!(self, Self::Float { .. })
    }
}

impl From<&DataType> for ArrowType {
    fn from(data_type: &DataType) -> Self {
        match data_type {
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int {
                signed: true,
                bits: 8,
            },
            DataType::Int16 => Self::Int {
                signed: true,
                bits: 16,
            },
            DataType::Int32 => Self::Int {
                signed: true,
                bits: 32,
            },
            DataType::Int64 => Self::Int {
                signed: true,
                bits: 64,
            },
            DataType::UInt8 => Self::Int {
                signed: false,
                bits: 8,
            },
            DataType::UInt16 => Self::Int {
                signed: false,
                bits: 16,
            },
            DataType::UInt32 => Self::Int {
                signed: false,
                bits: 32,
            },
            DataType::UInt64 => Self::Int {
                signed: false,
                bits: 64,
            },
            DataType::Float16 => Self::Float { bits: 16 },
            DataType::Float32 => Self::Float { bits: 32 },
            DataType::Float64 => Self::Float { bits: 64 },
            DataType::Decimal32(precision, scale) => Self::Decimal {
                bits: 32,
                precision: *precision,
                scale: *scale,
            },
            DataType::Decimal64(precision, scale) => Self::Decimal {
                bits: 64,
                precision: *precision,
                scale: *scale,
            },
            DataType::Decimal128(precision, scale) => Self::Decimal {
                bits: 128,
                precision: *precision,
                scale: *scale,
            },
            DataType::Decimal256(precision, scale) => Self::Decimal {
                bits: 256,
                precision: *precision,
                scale: *scale,
            },
            DataType::Timestamp(unit, timezone) => Self::Timestamp {
                unit: TimeUnitName::from(unit),
                timezone: timezone.as_ref().map(ToString::to_string),
            },
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Self::Utf8,
            DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_) => Self::Binary,
            DataType::Struct(_) => Self::Struct,
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_) => Self::List,
            DataType::Map(_, _) => Self::Map,
            other => Self::Other {
                display: other.to_string(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeUnitName {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl From<&TimeUnit> for TimeUnitName {
    fn from(unit: &TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => Self::Second,
            TimeUnit::Millisecond => Self::Millisecond,
            TimeUnit::Microsecond => Self::Microsecond,
            TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}

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
pub struct NormalizedSchema {
    pub fields: Vec<NormalizedField>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedField {
    pub source_name: String,
    pub output_name: String,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypeMappingDecision {
    AllowedLossless,
    AllowedLossyByContract,
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

pub fn compile_validation_program(
    policy: &ContractPolicy,
    observed_schema: &ObservedSchema,
) -> Result<ValidationProgram> {
    validate_normalizer(&policy.normalization.identifier)?;
    let normalized_schema = normalize_schema(observed_schema, &policy.normalization.identifier)?;

    let mut column_programs = Vec::with_capacity(observed_schema.fields.len());
    for (field, normalized) in observed_schema
        .fields
        .iter()
        .zip(normalized_schema.fields.iter())
    {
        validate_type_fidelity(policy, field)?;

        let mut steps = Vec::new();
        if policy.types.preserve_decimal_exactness {
            steps.push(ColumnProgramStep::PreserveDecimalExactness);
        }
        if policy.types.preserve_timestamp_timezone {
            steps.push(ColumnProgramStep::PreserveTimestampTimezone);
        }
        steps.extend(
            policy
                .transforms
                .iter()
                .filter(|transform| transform.column_name() == Some(field.source_name.as_str()))
                .cloned()
                .map(ColumnProgramStep::ApplyTransform),
        );

        column_programs.push(ColumnProgram {
            source_name: field.source_name.clone(),
            output_name: normalized.output_name.clone(),
            arrow_type: field.arrow_type.clone(),
            steps,
            nested_action: nested_action_for_field(
                &field.source_name,
                &field.arrow_type,
                &policy.normalization,
            )?,
            redaction: redaction_decision_for_semantic(
                field
                    .metadata
                    .get(firn_kernel::SEMANTIC_METADATA_KEY)
                    .map(String::as_str),
                &policy.quarantine.pii_redaction,
            ),
        });
    }

    Ok(ValidationProgram {
        normalizer_version: policy.normalization.identifier.version.clone(),
        schema_verdicts: schema_verdicts(&policy.schema, &policy.normalization.nested),
        column_programs,
        row_dispositions: row_dispositions(policy),
        transforms: policy.transforms.clone(),
        promotion: policy.promotion.clone(),
        warnings: Vec::new(),
    })
}

pub fn normalize_schema(
    observed_schema: &ObservedSchema,
    policy: &IdentifierPolicy,
) -> Result<NormalizedSchema> {
    validate_normalizer(policy)?;
    let mut outputs = BTreeMap::<String, String>::new();
    let mut fields = Vec::with_capacity(observed_schema.fields.len());

    for field in &observed_schema.fields {
        let output_name = normalize_identifier(&field.source_name, policy)?;
        if let Some(previous_source) =
            outputs.insert(output_name.clone(), field.source_name.clone())
        {
            return Err(FirnError::contract(format!(
                "identifier collision after {NORMALIZER_NAMECASE_V1}: {previous_source:?} and {:?} both normalize to {output_name:?}; add an explicit rename",
                field.source_name
            )));
        }

        let mut metadata = field.metadata.clone();
        metadata.insert(
            firn_kernel::SOURCE_NAME_METADATA_KEY.to_owned(),
            field.source_name.clone(),
        );
        fields.push(NormalizedField {
            source_name: field.source_name.clone(),
            output_name,
            metadata,
        });
    }

    Ok(NormalizedSchema { fields })
}

pub fn normalize_arrow_schema(schema: &Schema, policy: &IdentifierPolicy) -> Result<Schema> {
    let observed = ObservedSchema::from_arrow(schema);
    let normalized = normalize_schema(&observed, policy)?;
    let fields = schema
        .fields()
        .iter()
        .zip(normalized.fields)
        .map(|(field_ref, normalized)| {
            with_source_name(field_ref.as_ref().clone(), normalized.source_name)
                .with_name(normalized.output_name)
        })
        .collect::<Vec<_>>();

    Ok(Schema::new(fields))
}

pub fn normalize_identifier(source_name: &str, policy: &IdentifierPolicy) -> Result<String> {
    validate_normalizer(policy)?;
    let nfc = source_name.nfc().collect::<String>();
    let snake = lower_snake_case(&nfc);
    let filtered = filter_identifier_charset(&snake, &policy.charset);
    truncate_identifier(&filtered, source_name, policy.max_length)
}

pub fn redaction_decision_for_field(
    field: &Field,
    policy: &PiiRedactionPolicy,
) -> RedactionDecision {
    redaction_decision_for_semantic(semantic(field), policy)
}

pub fn redaction_decision_for_semantic(
    semantic: Option<&str>,
    policy: &PiiRedactionPolicy,
) -> RedactionDecision {
    match semantic {
        Some(tag) if tag.starts_with("pii:") => policy.pii_action.clone(),
        _ => policy.default_action.clone(),
    }
}

pub fn validate_type_mapping(
    policy: &ContractPolicy,
    mapping: &TypeMapping,
) -> Result<TypeMappingDecision> {
    match mapping.fidelity {
        TypeMappingFidelity::Lossless => Ok(TypeMappingDecision::AllowedLossless),
        TypeMappingFidelity::LossyRequiresContractAllowance if policy.types.allow_lossy_mapping => {
            Ok(TypeMappingDecision::AllowedLossyByContract)
        }
        TypeMappingFidelity::LossyRequiresContractAllowance => Err(FirnError::contract(format!(
            "lossy destination mapping from {} to {} requires allow_lossy_mapping",
            mapping.arrow_type, mapping.destination_type
        ))),
        TypeMappingFidelity::Unsupported => Err(FirnError::contract(format!(
            "unsupported destination mapping from {} to {}",
            mapping.arrow_type, mapping.destination_type
        ))),
    }
}

fn validate_type_fidelity(policy: &ContractPolicy, field: &ObservedField) -> Result<()> {
    if policy.types.preserve_decimal_exactness
        && let Some(SourceTypeClaim::Decimal { precision, scale }) = field.source_type
        && field.arrow_type.is_float()
    {
        return Err(FirnError::contract(format!(
            "decimal source field {:?} ({precision},{scale}) cannot compile as floating point",
            field.source_name
        )));
    }

    if policy.types.preserve_timestamp_timezone
        && let Some(SourceTypeClaim::Timestamp { timezone }) = &field.source_type
    {
        validate_timestamp_timezone(field, timezone)?;
    }

    Ok(())
}

fn validate_timestamp_timezone(field: &ObservedField, claim: &TimestampZoneClaim) -> Result<()> {
    let ArrowType::Timestamp { timezone, .. } = &field.arrow_type else {
        return Ok(());
    };

    match (claim, timezone) {
        (TimestampZoneClaim::Zoned { zone }, None) => Err(FirnError::contract(format!(
            "zoned timestamp field {:?} from zone {zone:?} lost its timezone",
            field.source_name
        ))),
        (TimestampZoneClaim::Naive, Some(observed_zone)) => Err(FirnError::contract(format!(
            "naive timestamp field {:?} cannot be silently assumed as timezone {observed_zone:?}",
            field.source_name
        ))),
        _ => Ok(()),
    }
}

fn schema_verdicts(
    schema: &SchemaPolicy,
    nested_policy: &NestedDataPolicy,
) -> Vec<SchemaVerdictRule> {
    vec![
        SchemaVerdictRule {
            change: SchemaChangeKind::NewTable,
            verdict: if schema.allow_new_table {
                VerdictAction::Admit
            } else {
                VerdictAction::RejectRun
            },
        },
        SchemaVerdictRule {
            change: SchemaChangeKind::NewColumn,
            verdict: if schema.allow_new_column {
                VerdictAction::Admit
            } else {
                VerdictAction::RejectRun
            },
        },
        SchemaVerdictRule {
            change: SchemaChangeKind::TypeWidening,
            verdict: if schema.allow_type_widening {
                VerdictAction::Admit
            } else {
                VerdictAction::RejectBatch
            },
        },
        SchemaVerdictRule {
            change: SchemaChangeKind::TypeNarrowing,
            verdict: if schema.quarantine_type_narrowing {
                VerdictAction::Quarantine
            } else {
                VerdictAction::RejectRun
            },
        },
        SchemaVerdictRule {
            change: SchemaChangeKind::UnknownField,
            verdict: if matches!(nested_policy, NestedDataPolicy::VariantCapture(_)) {
                VerdictAction::AdmitAsVariant
            } else if schema.allow_unknown_fields {
                VerdictAction::Admit
            } else {
                VerdictAction::RejectRun
            },
        },
    ]
}

fn row_dispositions(policy: &ContractPolicy) -> Vec<RowDispositionRule> {
    let violation = if policy.quarantine.enabled
        && matches!(policy.verdicts.violation, VerdictAction::Quarantine)
    {
        RowDispositionKind::Quarantine
    } else {
        action_to_row_disposition(&policy.verdicts.violation)
    };

    vec![
        RowDispositionRule {
            outcome: RuleOutcome::Pass,
            disposition: RowDispositionKind::Accept,
        },
        RowDispositionRule {
            outcome: RuleOutcome::Coerced,
            disposition: RowDispositionKind::Accept,
        },
        RowDispositionRule {
            outcome: RuleOutcome::AdmittedAsVariant,
            disposition: RowDispositionKind::Accept,
        },
        RowDispositionRule {
            outcome: RuleOutcome::Violation,
            disposition: violation,
        },
        RowDispositionRule {
            outcome: RuleOutcome::Fatal,
            disposition: action_to_row_disposition(&policy.verdicts.fatal),
        },
    ]
}

fn action_to_row_disposition(action: &VerdictAction) -> RowDispositionKind {
    match action {
        VerdictAction::Admit | VerdictAction::AdmitAsVariant => RowDispositionKind::Accept,
        VerdictAction::Quarantine => RowDispositionKind::Quarantine,
        VerdictAction::RejectBatch => RowDispositionKind::RejectBatch,
        VerdictAction::RejectRun => RowDispositionKind::RejectRun,
    }
}

fn nested_action_for_field(
    source_name: &str,
    arrow_type: &ArrowType,
    policy: &NormalizationPolicy,
) -> Result<NestedAction> {
    if !arrow_type.is_nested() {
        return Ok(NestedAction::NotNested);
    }

    match &policy.nested {
        NestedDataPolicy::KeepNested => Ok(NestedAction::KeepNested),
        NestedDataPolicy::ChildTableExpansion { .. } => Ok(NestedAction::ExpandToChildTable {
            child_table: normalize_identifier(source_name, &policy.identifier)?,
        }),
        NestedDataPolicy::VariantCapture(spec) => Ok(NestedAction::CaptureVariant {
            column_name: spec.column_name.clone(),
            semantic: spec.semantic.clone(),
        }),
    }
}

fn validate_normalizer(policy: &IdentifierPolicy) -> Result<()> {
    if policy.version != NORMALIZER_NAMECASE_V1 {
        return Err(FirnError::contract(format!(
            "unsupported identifier normalizer {:?}; expected {NORMALIZER_NAMECASE_V1:?}",
            policy.version
        )));
    }
    if policy.max_length < 10 {
        return Err(FirnError::contract(
            "identifier max_length must leave room for hash suffix",
        ));
    }
    Ok(())
}

fn lower_snake_case(input: &str) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    let mut out = String::new();
    let mut previous_was_word = false;
    let mut previous_was_lower_or_digit = false;

    for (index, ch) in chars.iter().copied().enumerate() {
        if !ch.is_alphanumeric() {
            push_separator(&mut out);
            previous_was_word = false;
            previous_was_lower_or_digit = false;
            continue;
        }

        let next_is_lower = chars.get(index + 1).is_some_and(|next| next.is_lowercase());
        if ch.is_uppercase() && previous_was_word && (previous_was_lower_or_digit || next_is_lower)
        {
            push_separator(&mut out);
        }

        for lower in ch.to_lowercase() {
            out.push(lower);
        }
        previous_was_word = true;
        previous_was_lower_or_digit = ch.is_lowercase() || ch.is_numeric();
    }

    trim_identifier_separators(out)
}

fn filter_identifier_charset(input: &str, charset: &IdentifierCharset) -> String {
    match charset {
        IdentifierCharset::AsciiLowerSnake => {
            let mut out = String::new();
            for ch in input.chars() {
                if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
                    out.push(ch);
                } else {
                    push_separator(&mut out);
                }
            }
            let filtered = trim_identifier_separators(out);
            if filtered.is_empty() {
                "field".to_owned()
            } else {
                filtered
            }
        }
    }
}

fn truncate_identifier(normalized: &str, source_name: &str, max_length: u16) -> Result<String> {
    let max_length = usize::from(max_length);
    if normalized.len() <= max_length {
        return Ok(normalized.to_owned());
    }

    if max_length < 10 {
        return Err(FirnError::contract(
            "identifier max_length must leave room for hash suffix",
        ));
    }

    let prefix_len = max_length - 9;
    let prefix = normalized.chars().take(prefix_len).collect::<String>();
    Ok(format!(
        "{}_{}",
        prefix.trim_end_matches('_'),
        hash8(source_name)
    ))
}

fn push_separator(out: &mut String) {
    if !out.is_empty() && !out.ends_with('_') {
        out.push('_');
    }
}

fn trim_identifier_separators(mut value: String) -> String {
    while value.ends_with('_') {
        value.pop();
    }
    value
}

fn hash8(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{:08x}", hash as u32)
}

trait TransformColumn {
    fn column_name(&self) -> Option<&str>;
}

impl TransformColumn for TransformDescription {
    fn column_name(&self) -> Option<&str> {
        match self {
            Self::Rename { from, .. } => Some(from.as_str()),
            Self::Cast { column, .. }
            | Self::Redact { column, .. }
            | Self::Derive { column, .. }
            | Self::ExpandNested { column, .. } => Some(column.as_str()),
            Self::Filter { .. } => None,
        }
    }
}

pub fn assert_verdict_lattice_total(program: &ValidationProgram) -> Result<()> {
    let covered = program
        .row_dispositions
        .iter()
        .map(|rule| rule.outcome)
        .collect::<BTreeSet<_>>();
    for outcome in RuleOutcome::ALL {
        if !covered.contains(&outcome) {
            return Err(FirnError::contract(format!(
                "validation program lacks a row disposition for {outcome:?}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use firn_kernel::{TypeMappingFidelity, with_semantic, with_source_name};

    #[test]
    fn validation_program_serializes_and_has_total_lattice() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let observed = ObservedSchema::from_arrow(&schema);
        let program =
            compile_validation_program(&ContractPolicy::for_trust(TrustLevel::Governed), &observed)
                .unwrap();

        assert_verdict_lattice_total(&program).unwrap();
        for outcome in RuleOutcome::ALL {
            assert_ne!(
                program.disposition_for(outcome, "rule-1"),
                RuleDisposition::RejectRun {
                    rule_id: "missing".to_owned()
                }
            );
        }

        let json = serde_json::to_string(&program).unwrap();
        assert_eq!(
            program,
            serde_json::from_str::<ValidationProgram>(&json).unwrap()
        );
    }

    #[test]
    fn decimal_fidelity_rejects_silent_float_conversion() {
        let schema = Schema::new(vec![Field::new("amount", DataType::Float64, false)]);
        let mut claims = BTreeMap::new();
        claims.insert(
            "amount".to_owned(),
            SourceTypeClaim::Decimal {
                precision: 18,
                scale: 2,
            },
        );
        let observed = ObservedSchema::from_arrow_with_claims(&schema, claims);

        let error = compile_validation_program(
            &ContractPolicy::for_trust(TrustLevel::Financial),
            &observed,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("cannot compile as floating point")
        );
    }

    #[test]
    fn timestamp_fidelity_rejects_naive_utc_assumption() {
        let schema = Schema::new(vec![Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )]);
        let mut claims = BTreeMap::new();
        claims.insert(
            "created_at".to_owned(),
            SourceTypeClaim::Timestamp {
                timezone: TimestampZoneClaim::Naive,
            },
        );
        let observed = ObservedSchema::from_arrow_with_claims(&schema, claims);

        let error = compile_validation_program(
            &ContractPolicy::for_trust(TrustLevel::Financial),
            &observed,
        )
        .unwrap_err();

        assert!(error.to_string().contains("cannot be silently assumed"));
    }

    #[test]
    fn timestamp_fidelity_rejects_lost_zoned_story() {
        let schema = Schema::new(vec![Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]);
        let mut claims = BTreeMap::new();
        claims.insert(
            "created_at".to_owned(),
            SourceTypeClaim::Timestamp {
                timezone: TimestampZoneClaim::Zoned {
                    zone: "America/Phoenix".to_owned(),
                },
            },
        );
        let observed = ObservedSchema::from_arrow_with_claims(&schema, claims);

        let error = compile_validation_program(
            &ContractPolicy::for_trust(TrustLevel::Financial),
            &observed,
        )
        .unwrap_err();

        assert!(error.to_string().contains("lost its timezone"));
    }

    #[test]
    fn normalizer_preserves_source_names_and_rejects_collisions() {
        let schema = Schema::new(vec![
            Field::new("userName", DataType::Utf8, true),
            Field::new("user_name", DataType::Utf8, true),
        ]);
        let observed = ObservedSchema::from_arrow(&schema);
        let error = normalize_schema(&observed, &IdentifierPolicy::default()).unwrap_err();
        assert!(error.to_string().contains("identifier collision"));

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("id", DataType::Utf8, true),
        ]);
        let error = normalize_schema(
            &ObservedSchema::from_arrow(&schema),
            &IdentifierPolicy::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("identifier collision"));

        let source_field = with_source_name(
            Field::new("already_normalized", DataType::Utf8, true),
            "Original Name",
        );
        let schema = Schema::new(vec![source_field]);
        let normalized = normalize_arrow_schema(&schema, &IdentifierPolicy::default()).unwrap();
        let field = normalized.field(0);
        assert_eq!(field.name(), "original_name");
        assert_eq!(source_name(field), Some("Original Name"));
    }

    #[test]
    fn namecase_v1_truncates_with_stable_hash_suffix() {
        let policy = IdentifierPolicy {
            max_length: 20,
            ..IdentifierPolicy::default()
        };
        let normalized = normalize_identifier("Very Long Source Identifier Name", &policy).unwrap();

        assert_eq!(normalized.len(), 20);
        assert!(normalized.contains('_'));
        assert_eq!(
            normalized,
            normalize_identifier("Very Long Source Identifier Name", &policy).unwrap()
        );
    }

    #[test]
    fn trust_presets_expand_to_specified_policy_shapes() {
        let experimental = ContractPolicy::for_trust(TrustLevel::Experimental);
        assert_eq!(experimental.schema.mode, SchemaEvolutionMode::Evolve);
        assert!(!experimental.quarantine.enabled);
        assert!(matches!(
            experimental.normalization.nested,
            NestedDataPolicy::VariantCapture(_)
        ));

        let governed = ContractPolicy::for_trust(TrustLevel::Governed);
        assert_eq!(governed.schema.mode, SchemaEvolutionMode::Evolve);
        assert!(governed.schema.review_artifact_required);
        assert_eq!(governed.rows.validation_depth, ValidationDepth::Full);
        assert!(governed.quarantine.enabled);

        let financial = ContractPolicy::for_trust(TrustLevel::Financial);
        assert_eq!(financial.schema.mode, SchemaEvolutionMode::Freeze);
        assert!(financial.types.preserve_decimal_exactness);
        assert!(financial.types.preserve_timestamp_timezone);
        assert_eq!(financial.lineage, LineagePolicy::Full);
        assert!(financial.receipts_required);
        assert!(financial.reconciliation_counts);
        assert_eq!(financial.retention, RetentionClass::Long);

        let serving = ContractPolicy::for_trust(TrustLevel::Serving);
        assert_eq!(serving.schema.mode, SchemaEvolutionMode::Freeze);
        assert!(serving.rows.freshness_slo);
        assert!(matches!(
            serving.rows.validation_depth,
            ValidationDepth::SampledFastPath { .. }
        ));
        assert!(serving.promotion.demote_on_anomaly);
    }

    #[test]
    fn nested_variant_policy_compiles_variant_capture_action() {
        let schema = Schema::new(vec![Field::new_struct(
            "payload",
            vec![Field::new("id", DataType::Int64, false)],
            true,
        )]);
        let observed = ObservedSchema::from_arrow(&schema);
        let program = compile_validation_program(
            &ContractPolicy::for_trust(TrustLevel::Experimental),
            &observed,
        )
        .unwrap();

        assert_eq!(
            program.column_programs[0].nested_action,
            NestedAction::CaptureVariant {
                column_name: VARIANT_COLUMN_NAME.to_owned(),
                semantic: VARIANT_SEMANTIC_TAG.to_owned(),
            }
        );
        assert!(program.schema_verdicts.iter().any(|rule| {
            rule.change == SchemaChangeKind::UnknownField
                && rule.verdict == VerdictAction::AdmitAsVariant
        }));
    }

    #[test]
    fn pii_redaction_decision_is_available_from_semantic_metadata() {
        let field = with_semantic(Field::new("email", DataType::Utf8, false), "pii:email");
        let decision = redaction_decision_for_field(&field, &PiiRedactionPolicy::default());

        assert_eq!(
            decision,
            RedactionDecision::Hash {
                algorithm: "sha256".to_owned(),
            }
        );
    }

    #[test]
    fn lossy_mapping_requires_explicit_policy_allowance() {
        let mapping = TypeMapping {
            arrow_type: "Decimal128(18, 2)".to_owned(),
            destination_type: "DOUBLE".to_owned(),
            fidelity: TypeMappingFidelity::LossyRequiresContractAllowance,
        };

        let denied = validate_type_mapping(&ContractPolicy::default(), &mapping).unwrap_err();
        assert!(denied.to_string().contains("requires allow_lossy_mapping"));

        let mut allowed_policy = ContractPolicy::default();
        allowed_policy.types.allow_lossy_mapping = true;
        assert_eq!(
            validate_type_mapping(&allowed_policy, &mapping).unwrap(),
            TypeMappingDecision::AllowedLossyByContract
        );
    }
}
