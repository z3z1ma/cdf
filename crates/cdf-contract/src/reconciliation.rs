use std::collections::{BTreeMap, BTreeSet};

use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{
    CdfError, Result, physical_type, source_name, with_physical_type, with_source_name,
};
use serde::{Deserialize, Serialize};

use crate::{policy::TypePolicy, program::RuleOutcome};

const SCHEMA_COERCION_PLAN_METADATA_KEY: &str = "cdf:schema_coercion_plan";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaReconciliation {
    pub schema: Schema,
    pub plan: SchemaCoercionPlan,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaReconciliationReport {
    pub schema: Option<Schema>,
    pub plan: SchemaCoercionPlan,
    pub errors: Vec<SchemaReconciliationError>,
}

impl SchemaReconciliationReport {
    pub fn into_result(self) -> Result<SchemaReconciliation> {
        match (self.schema, self.errors.is_empty()) {
            (Some(schema), true) => Ok(SchemaReconciliation {
                schema,
                plan: self.plan,
            }),
            (_, false) => Err(CdfError::contract(reconciliation_error_message(
                &self.errors,
            ))),
            (None, true) => Err(CdfError::internal(
                "schema reconciliation produced no schema and no error",
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaCoercionPlan {
    pub fields: Vec<FieldCoercion>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldCoercion {
    pub source_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constraint_type: Option<String>,
    pub decision: FieldCoercionDecision,
    pub outcome: RuleOutcome,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operator_fixes: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldCoercionDecision {
    Preserved,
    Widened,
    CoercedByPolicy,
    LossyAllowed,
    LossyRejected,
    Unsupported,
    Missing,
    Extra,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaReconciliationError {
    pub source_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constraint_type: Option<String>,
    pub message: String,
    pub operator_fixes: Vec<String>,
}

pub fn reconcile_schema(
    observed: &Schema,
    constraint: &Schema,
    type_policy: &TypePolicy,
) -> Result<SchemaReconciliation> {
    plan_schema_reconciliation(observed, constraint, type_policy)?.into_result()
}

pub fn schema_coercion_plan_from_reconciled_schema(
    schema: &Schema,
) -> Result<Option<SchemaCoercionPlan>> {
    let Some(serialized) = schema.metadata().get(SCHEMA_COERCION_PLAN_METADATA_KEY) else {
        return Ok(None);
    };
    let plan = parse_schema_coercion_plan(serialized)?;
    validate_schema_coercion_plan(schema, &plan)?;
    Ok(Some(plan))
}

pub fn schema_coercion_plan_from_trusted_json(
    schema: &Schema,
    serialized: &str,
) -> Result<SchemaCoercionPlan> {
    let plan = parse_schema_coercion_plan(serialized)?;
    let metadata_plan = schema
        .metadata()
        .get(SCHEMA_COERCION_PLAN_METADATA_KEY)
        .ok_or_else(|| {
            invalid_coercion_evidence(
                "trusted batch evidence has no matching reserved Arrow schema metadata",
            )
        })?;
    let metadata_plan = parse_schema_coercion_plan(metadata_plan)?;
    if metadata_plan != plan {
        return Err(invalid_coercion_evidence(
            "trusted batch evidence does not match Arrow schema metadata",
        ));
    }
    validate_schema_coercion_plan(schema, &plan)?;
    Ok(plan)
}

pub fn reject_untrusted_schema_coercion_metadata(schema: &Schema) -> Result<()> {
    if schema
        .metadata()
        .contains_key(SCHEMA_COERCION_PLAN_METADATA_KEY)
    {
        return Err(invalid_coercion_evidence(
            "Arrow schema carries coercion-plan metadata without trusted batch evidence",
        ));
    }
    Ok(())
}

fn parse_schema_coercion_plan(serialized: &str) -> Result<SchemaCoercionPlan> {
    serde_json::from_str(serialized).map_err(|error| {
        invalid_coercion_evidence(format!("metadata is not a valid coercion plan: {error}"))
    })
}

fn validate_schema_coercion_plan(schema: &Schema, plan: &SchemaCoercionPlan) -> Result<()> {
    let mut seen_sources = BTreeSet::new();
    let mut output_index = 0_usize;
    let mut saw_extra = false;
    let mut previous_extra = None::<&str>;

    for decision in &plan.fields {
        if !seen_sources.insert(decision.source_name.as_str()) {
            return Err(invalid_coercion_evidence(format!(
                "duplicate source field {:?}",
                decision.source_name
            )));
        }

        match decision.output_name.as_deref() {
            Some(output_name) => {
                if saw_extra {
                    return Err(invalid_coercion_evidence(
                        "output field decision appears after an extra-field decision",
                    ));
                }
                let Some(field) = schema.fields().get(output_index) else {
                    return Err(invalid_coercion_evidence(format!(
                        "plan contains unexpected output field {output_name:?}"
                    )));
                };
                validate_output_field_decision(field.as_ref(), decision)?;
                output_index += 1;
            }
            None => {
                saw_extra = true;
                validate_extra_field_decision(decision)?;
                if let Some(previous) = previous_extra
                    && previous >= decision.source_name.as_str()
                {
                    return Err(invalid_coercion_evidence(
                        "extra-field decisions are not in deterministic source-name order",
                    ));
                }
                previous_extra = Some(decision.source_name.as_str());
            }
        }
    }

    if output_index != schema.fields().len() {
        return Err(invalid_coercion_evidence(format!(
            "plan covers {output_index} output fields but schema has {}",
            schema.fields().len()
        )));
    }
    Ok(())
}

fn validate_output_field_decision(field: &Field, decision: &FieldCoercion) -> Result<()> {
    let source = field_source_name(field);
    let observed = decision.observed_type.as_deref().ok_or_else(|| {
        invalid_coercion_evidence(format!("field {source:?} has no observed type"))
    })?;
    let constraint = decision.constraint_type.as_deref().ok_or_else(|| {
        invalid_coercion_evidence(format!("field {source:?} has no constraint type"))
    })?;
    let expected_observed = physical_type(field)
        .map(str::to_owned)
        .unwrap_or_else(|| field.data_type().to_string());

    if decision.source_name != source
        || decision.output_name.as_deref() != Some(field.name())
        || constraint != field.data_type().to_string()
        || observed != expected_observed
        || decision.observed_name.is_none()
        || !decision.operator_fixes.is_empty()
    {
        return Err(invalid_coercion_evidence(format!(
            "field {:?} does not match reconciled schema identity or types",
            decision.source_name
        )));
    }

    let (expected_outcome, expected_reason, relation_valid) = match decision.decision {
        FieldCoercionDecision::Preserved => (
            RuleOutcome::Pass,
            "observed type already satisfies the constraint".to_owned(),
            observed == constraint,
        ),
        FieldCoercionDecision::Widened => (
            RuleOutcome::Coerced,
            format!("lossless widening from {observed} to {constraint}"),
            observed != constraint && is_lossless_widening_display(observed, constraint),
        ),
        FieldCoercionDecision::CoercedByPolicy => (
            RuleOutcome::Coerced,
            format!("explicit coerce_types policy permits parsing from {observed} to {constraint}"),
            is_parse_coercion_display(observed, constraint),
        ),
        FieldCoercionDecision::LossyAllowed => (
            RuleOutcome::Coerced,
            format!("allow_lossy_mapping permits lossy cast from {observed} to {constraint}"),
            is_lossy_mapping_display(observed, constraint),
        ),
        FieldCoercionDecision::LossyRejected
        | FieldCoercionDecision::Unsupported
        | FieldCoercionDecision::Missing
        | FieldCoercionDecision::Extra => {
            return Err(invalid_coercion_evidence(format!(
                "field {source:?} carries non-success decision {:?}",
                decision.decision
            )));
        }
    };

    if !relation_valid || decision.outcome != expected_outcome || decision.reason != expected_reason
    {
        return Err(invalid_coercion_evidence(format!(
            "field {source:?} decision {:?} is inconsistent with {observed} -> {constraint}",
            decision.decision
        )));
    }
    Ok(())
}

fn validate_extra_field_decision(decision: &FieldCoercion) -> Result<()> {
    if decision.decision != FieldCoercionDecision::Extra
        || decision.outcome != RuleOutcome::AdmittedAsVariant
        || decision.observed_name.is_none()
        || decision.observed_type.is_none()
        || decision.constraint_type.is_some()
        || !decision.operator_fixes.is_empty()
        || decision.reason != "observed field is outside the constraint projection"
    {
        return Err(invalid_coercion_evidence(format!(
            "extra-field decision {:?} is structurally inconsistent",
            decision.source_name
        )));
    }
    Ok(())
}

fn is_parse_coercion_display(observed: &str, constraint: &str) -> bool {
    matches!(observed, "Utf8" | "LargeUtf8" | "Utf8View")
        && (is_numeric_display(constraint)
            || is_temporal_display(constraint)
            || constraint == "Boolean")
}

fn is_lossy_mapping_display(observed: &str, constraint: &str) -> bool {
    observed != constraint
        && !is_lossless_widening_display(observed, constraint)
        && !is_parse_coercion_display(observed, constraint)
        && ((is_numeric_display(observed) && is_numeric_display(constraint))
            || (observed.starts_with("Timestamp(") && matches!(constraint, "Date32" | "Date64"))
            || (observed == "Date64" && constraint == "Date32"))
}

fn is_numeric_display(value: &str) -> bool {
    matches!(
        value,
        "Int8"
            | "Int16"
            | "Int32"
            | "Int64"
            | "UInt8"
            | "UInt16"
            | "UInt32"
            | "UInt64"
            | "Float16"
            | "Float32"
            | "Float64"
    ) || value.starts_with("Decimal32(")
        || value.starts_with("Decimal64(")
        || value.starts_with("Decimal128(")
        || value.starts_with("Decimal256(")
}

fn is_temporal_display(value: &str) -> bool {
    matches!(value, "Date32" | "Date64")
        || value.starts_with("Time32(")
        || value.starts_with("Time64(")
        || value.starts_with("Timestamp(")
        || value.starts_with("Duration(")
}

fn invalid_coercion_evidence(message: impl Into<String>) -> CdfError {
    CdfError::data(format!(
        "invalid schema coercion evidence: {}",
        message.into()
    ))
}

fn is_lossless_widening_display(observed: &str, constraint: &str) -> bool {
    matches!(
        (observed, constraint),
        ("Int8", "Int16" | "Int32" | "Int64")
            | ("Int16", "Int32" | "Int64")
            | ("Int32", "Int64")
            | ("UInt8", "UInt16" | "UInt32" | "UInt64")
            | ("UInt16", "UInt32" | "UInt64")
            | ("UInt32", "UInt64")
            | ("Float32", "Float64")
    ) || (observed == "Date32" && constraint.starts_with("Timestamp("))
        || (is_integer_display(observed) && is_decimal_display(constraint))
}

fn is_integer_display(value: &str) -> bool {
    matches!(
        value,
        "Int8" | "Int16" | "Int32" | "Int64" | "UInt8" | "UInt16" | "UInt32" | "UInt64"
    )
}

fn is_decimal_display(value: &str) -> bool {
    value.starts_with("Decimal128(") || value.starts_with("Decimal256(")
}

pub fn plan_schema_reconciliation(
    observed: &Schema,
    constraint: &Schema,
    type_policy: &TypePolicy,
) -> Result<SchemaReconciliationReport> {
    let observed_by_source = fields_by_source_name(observed, "observed")?;
    let constraint_by_source = fields_by_source_name(constraint, "constraint")?;
    let mut matched_sources = BTreeSet::new();
    let mut output_fields = Vec::new();
    let mut decisions = Vec::new();
    let mut errors = Vec::new();

    for constraint_field_ref in constraint.fields() {
        let constraint_field = constraint_field_ref.as_ref();
        let field_source_name = field_source_name(constraint_field);
        let Some(observed_field) = observed_by_source.get(&field_source_name) else {
            let field_error = missing_field_error(&field_source_name, constraint_field.data_type());
            decisions.push(field_error.decision());
            errors.push(field_error);
            continue;
        };
        matched_sources.insert(field_source_name.clone());

        let type_decision = reconcile_type(
            observed_field.data_type(),
            constraint_field.data_type(),
            type_policy,
        );
        let field_decision =
            type_decision.field_decision(&field_source_name, observed_field, constraint_field);

        match type_decision {
            TypeReconciliation::Preserved
            | TypeReconciliation::Widened
            | TypeReconciliation::CoercedByPolicy
            | TypeReconciliation::LossyAllowed => {
                output_fields.push(reconciled_field(
                    constraint_field,
                    observed_field,
                    &field_source_name,
                    type_decision.requires_physical_provenance()
                        || field_identity_differs(observed_field, constraint_field),
                ));
            }
            TypeReconciliation::LossyRejected { .. } | TypeReconciliation::Unsupported => {
                errors.push(field_decision.error());
            }
        }

        decisions.push(field_decision);
    }

    for (source, observed_field) in observed_by_source {
        if matched_sources.contains(&source) || constraint_by_source.contains_key(&source) {
            continue;
        }
        decisions.push(extra_field_decision(&source, &observed_field));
    }

    let plan = SchemaCoercionPlan { fields: decisions };
    let schema = if errors.is_empty() {
        let serialized_plan = serde_json::to_string(&plan).map_err(|error| {
            CdfError::internal(format!("serialize schema coercion plan metadata: {error}"))
        })?;
        let mut metadata = constraint.metadata().clone();
        metadata.insert(
            SCHEMA_COERCION_PLAN_METADATA_KEY.to_owned(),
            serialized_plan,
        );
        Some(Schema::new_with_metadata(output_fields, metadata))
    } else {
        None
    };

    Ok(SchemaReconciliationReport {
        schema,
        plan,
        errors,
    })
}

fn fields_by_source_name(schema: &Schema, label: &str) -> Result<BTreeMap<String, Field>> {
    let mut fields = BTreeMap::new();
    for field_ref in schema.fields() {
        let field = field_ref.as_ref();
        let source = field_source_name(field);
        if fields.insert(source.clone(), field.clone()).is_some() {
            return Err(CdfError::contract(format!(
                "{label} schema contains duplicate source field {source:?}; schema reconciliation requires unique cdf:source_name values"
            )));
        }
    }
    Ok(fields)
}

fn field_source_name(field: &Field) -> String {
    source_name(field)
        .unwrap_or_else(|| field.name())
        .to_owned()
}

fn reconciled_field(
    constraint_field: &Field,
    observed_field: &Field,
    field_source_name: &str,
    include_physical_type: bool,
) -> Field {
    let mut field = constraint_field.clone();
    if source_name(&field).is_none() {
        field = with_source_name(field, field_source_name);
    }
    if include_physical_type {
        field = with_physical_type(field, observed_field.data_type().to_string());
    }
    field
}

fn field_identity_differs(observed_field: &Field, constraint_field: &Field) -> bool {
    observed_field.name() != constraint_field.name()
        || observed_field.is_nullable() != constraint_field.is_nullable()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TypeReconciliation {
    Preserved,
    Widened,
    CoercedByPolicy,
    LossyAllowed,
    LossyRejected { allowance: ExplicitAllowance },
    Unsupported,
}

impl TypeReconciliation {
    fn requires_physical_provenance(self) -> bool {
        !matches!(self, Self::Preserved)
    }

    fn field_decision(
        self,
        field_source_name: &str,
        observed_field: &Field,
        constraint_field: &Field,
    ) -> FieldCoercion {
        let observed_type = observed_field.data_type().to_string();
        let constraint_type = constraint_field.data_type().to_string();
        let (decision, outcome, reason, operator_fixes) = match self {
            Self::Preserved => (
                FieldCoercionDecision::Preserved,
                RuleOutcome::Pass,
                "observed type already satisfies the constraint".to_owned(),
                Vec::new(),
            ),
            Self::Widened => (
                FieldCoercionDecision::Widened,
                RuleOutcome::Coerced,
                format!("lossless widening from {observed_type} to {constraint_type}"),
                Vec::new(),
            ),
            Self::CoercedByPolicy => (
                FieldCoercionDecision::CoercedByPolicy,
                RuleOutcome::Coerced,
                format!(
                    "explicit coerce_types policy permits parsing from {observed_type} to {constraint_type}"
                ),
                Vec::new(),
            ),
            Self::LossyAllowed => (
                FieldCoercionDecision::LossyAllowed,
                RuleOutcome::Coerced,
                format!(
                    "allow_lossy_mapping permits lossy cast from {observed_type} to {constraint_type}"
                ),
                Vec::new(),
            ),
            Self::LossyRejected { allowance } => (
                FieldCoercionDecision::LossyRejected,
                RuleOutcome::Fatal,
                format!("lossy cast from {observed_type} to {constraint_type} is not allowed"),
                allowance.operator_fixes(&observed_type),
            ),
            Self::Unsupported => (
                FieldCoercionDecision::Unsupported,
                RuleOutcome::Fatal,
                format!(
                    "unsupported schema reconciliation from {observed_type} to {constraint_type}"
                ),
                unsupported_operator_fixes(&observed_type),
            ),
        };

        FieldCoercion {
            source_name: field_source_name.to_owned(),
            observed_name: Some(observed_field.name().clone()),
            output_name: Some(constraint_field.name().clone()),
            observed_type: Some(observed_type),
            constraint_type: Some(constraint_type),
            decision,
            outcome,
            reason,
            operator_fixes,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExplicitAllowance {
    CoerceTypes,
    AllowLossyMapping,
}

impl ExplicitAllowance {
    fn operator_fixes(self, observed_type: &str) -> Vec<String> {
        match self {
            Self::CoerceTypes => vec![
                format!("change the declaration to {observed_type}"),
                "enable coerce_types for this resource if parsing is intentional".to_owned(),
            ],
            Self::AllowLossyMapping => vec![
                format!("widen or change the declaration to {observed_type}"),
                "enable allow_lossy_mapping for this resource if the lossy cast is intentional"
                    .to_owned(),
            ],
        }
    }
}

fn reconcile_type(
    observed: &DataType,
    constraint: &DataType,
    type_policy: &TypePolicy,
) -> TypeReconciliation {
    if observed == constraint {
        return TypeReconciliation::Preserved;
    }
    if is_lossless_type_widening(observed, constraint) {
        return TypeReconciliation::Widened;
    }
    if is_parse_coercion(observed, constraint) {
        return if type_policy.coerce_types {
            TypeReconciliation::CoercedByPolicy
        } else {
            TypeReconciliation::LossyRejected {
                allowance: ExplicitAllowance::CoerceTypes,
            }
        };
    }
    if is_lossy_mapping(observed, constraint) {
        return if type_policy.allow_lossy_mapping {
            TypeReconciliation::LossyAllowed
        } else {
            TypeReconciliation::LossyRejected {
                allowance: ExplicitAllowance::AllowLossyMapping,
            }
        };
    }
    TypeReconciliation::Unsupported
}

pub fn is_lossless_type_widening(observed: &DataType, constraint: &DataType) -> bool {
    if let (Some(observed_bits), Some(constraint_bits)) = (
        signed_integer_bits(observed),
        signed_integer_bits(constraint),
    ) {
        return observed_bits < constraint_bits;
    }
    if let (Some(observed_bits), Some(constraint_bits)) = (
        unsigned_integer_bits(observed),
        unsigned_integer_bits(constraint),
    ) {
        return observed_bits < constraint_bits;
    }

    matches!(
        (observed, constraint),
        (DataType::Float32, DataType::Float64)
    ) || integer_to_decimal_widening(observed, constraint)
        || matches!(
            (observed, constraint),
            (DataType::Date32, DataType::Timestamp(_, _))
        )
}

fn is_parse_coercion(observed: &DataType, constraint: &DataType) -> bool {
    is_utf8_type(observed)
        && (is_numeric_type(constraint)
            || is_temporal_type(constraint)
            || matches!(constraint, DataType::Boolean))
}

fn is_lossy_mapping(observed: &DataType, constraint: &DataType) -> bool {
    if let (Some((observed_signed, observed_bits)), Some((constraint_signed, constraint_bits))) =
        (integer_parts(observed), integer_parts(constraint))
    {
        return observed_signed != constraint_signed || observed_bits > constraint_bits;
    }

    matches!(
        (observed, constraint),
        (DataType::Float64, DataType::Float32 | DataType::Float16)
            | (DataType::Float32, DataType::Float16)
            | (
                DataType::Timestamp(_, _),
                DataType::Date32 | DataType::Date64
            )
            | (DataType::Date64, DataType::Date32)
    ) || (is_numeric_type(observed) && is_numeric_type(constraint))
}

fn integer_to_decimal_widening(observed: &DataType, constraint: &DataType) -> bool {
    let Some((signed, bits)) = integer_parts(observed) else {
        return false;
    };
    let (DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale)) =
        constraint
    else {
        return false;
    };
    decimal_can_hold_integer_domain(signed, bits, *precision, *scale)
}

fn signed_integer_bits(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 => Some(8),
        DataType::Int16 => Some(16),
        DataType::Int32 => Some(32),
        DataType::Int64 => Some(64),
        _ => None,
    }
}

fn unsigned_integer_bits(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::UInt8 => Some(8),
        DataType::UInt16 => Some(16),
        DataType::UInt32 => Some(32),
        DataType::UInt64 => Some(64),
        _ => None,
    }
}

fn integer_parts(data_type: &DataType) -> Option<(bool, u8)> {
    signed_integer_bits(data_type)
        .map(|bits| (true, bits))
        .or_else(|| unsigned_integer_bits(data_type).map(|bits| (false, bits)))
}

fn decimal_can_hold_integer_domain(signed: bool, bits: u8, precision: u8, scale: i8) -> bool {
    if scale < 0 {
        return false;
    }
    let Some(domain_digits) = integer_domain_digits(signed, bits) else {
        return false;
    };
    precision as u16 >= domain_digits as u16 + scale as u16
}

fn integer_domain_digits(signed: bool, bits: u8) -> Option<u8> {
    match (signed, bits) {
        (true, 8) | (false, 8) => Some(3),
        (true, 16) | (false, 16) => Some(5),
        (true, 32) | (false, 32) => Some(10),
        (true, 64) => Some(19),
        (false, 64) => Some(20),
        _ => None,
    }
}

fn is_utf8_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn is_temporal_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
    )
}

fn missing_field_error(source_name: &str, constraint_type: &DataType) -> SchemaReconciliationError {
    let constraint_type = constraint_type.to_string();
    SchemaReconciliationError {
        source_name: source_name.to_owned(),
        observed_type: None,
        constraint_type: Some(constraint_type.clone()),
        message: format!(
            "field {source_name:?} is declared as {constraint_type} but was not observed"
        ),
        operator_fixes: vec![
            "remove or rename the declaration if this field is no longer present".to_owned(),
            "fix the source or discovery probe so the field is present".to_owned(),
        ],
    }
}

impl SchemaReconciliationError {
    fn decision(&self) -> FieldCoercion {
        FieldCoercion {
            source_name: self.source_name.clone(),
            observed_name: None,
            output_name: Some(self.source_name.clone()),
            observed_type: self.observed_type.clone(),
            constraint_type: self.constraint_type.clone(),
            decision: FieldCoercionDecision::Missing,
            outcome: RuleOutcome::Fatal,
            reason: self.message.clone(),
            operator_fixes: self.operator_fixes.clone(),
        }
    }
}

impl FieldCoercion {
    fn error(&self) -> SchemaReconciliationError {
        SchemaReconciliationError {
            source_name: self.source_name.clone(),
            observed_type: self.observed_type.clone(),
            constraint_type: self.constraint_type.clone(),
            message: self.reason.clone(),
            operator_fixes: self.operator_fixes.clone(),
        }
    }
}

fn extra_field_decision(source_name: &str, observed_field: &Field) -> FieldCoercion {
    FieldCoercion {
        source_name: source_name.to_owned(),
        observed_name: Some(observed_field.name().clone()),
        output_name: None,
        observed_type: Some(observed_field.data_type().to_string()),
        constraint_type: None,
        decision: FieldCoercionDecision::Extra,
        outcome: RuleOutcome::AdmittedAsVariant,
        reason: "observed field is outside the constraint projection".to_owned(),
        operator_fixes: Vec::new(),
    }
}

fn unsupported_operator_fixes(observed_type: &str) -> Vec<String> {
    vec![
        format!("change the declaration to {observed_type}"),
        "choose a supported explicit transform before schema reconciliation".to_owned(),
    ]
}

fn reconciliation_error_message(errors: &[SchemaReconciliationError]) -> String {
    errors
        .iter()
        .map(|error| {
            let observed = error.observed_type.as_deref().unwrap_or("<missing>");
            let constraint = error.constraint_type.as_deref().unwrap_or("<none>");
            format!(
                "field {:?}: {}; observed type {}; declared type {}; fixes: {}",
                error.source_name,
                error.message,
                observed,
                constraint,
                error.operator_fixes.join("; ")
            )
        })
        .collect::<Vec<_>>()
        .join(" | ")
}
