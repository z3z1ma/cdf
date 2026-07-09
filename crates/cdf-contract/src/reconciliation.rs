use std::collections::{BTreeMap, BTreeSet};

use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{
    CdfError, Result, physical_type, source_name, with_physical_type, with_source_name,
};
use serde::{Deserialize, Serialize};

use crate::{policy::TypePolicy, program::RuleOutcome};

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

pub fn schema_coercion_plan_from_reconciled_schema(schema: &Schema) -> Option<SchemaCoercionPlan> {
    let has_physical_provenance = schema
        .fields()
        .iter()
        .any(|field| physical_type(field.as_ref()).is_some());
    if !has_physical_provenance {
        return None;
    }

    Some(SchemaCoercionPlan {
        fields: schema
            .fields()
            .iter()
            .map(|field| field_coercion_from_reconciled_field(field.as_ref()))
            .collect(),
    })
}

fn field_coercion_from_reconciled_field(field: &Field) -> FieldCoercion {
    let source = field_source_name(field);
    let observed_type = physical_type(field)
        .map(str::to_owned)
        .unwrap_or_else(|| field.data_type().to_string());
    let constraint_type = field.data_type().to_string();
    let (decision, outcome, reason) = if observed_type == constraint_type {
        (
            FieldCoercionDecision::Preserved,
            RuleOutcome::Pass,
            "observed type already satisfies the constraint".to_owned(),
        )
    } else if is_lossless_widening_display(&observed_type, &constraint_type) {
        (
            FieldCoercionDecision::Widened,
            RuleOutcome::Coerced,
            format!("lossless widening from {observed_type} to {constraint_type}"),
        )
    } else {
        (
            FieldCoercionDecision::CoercedByPolicy,
            RuleOutcome::Coerced,
            format!(
                "reconciled schema metadata records physical type {observed_type} for output type {constraint_type}"
            ),
        )
    };

    FieldCoercion {
        source_name: source.clone(),
        observed_name: Some(source),
        output_name: Some(field.name().clone()),
        observed_type: Some(observed_type),
        constraint_type: Some(constraint_type),
        decision,
        outcome,
        reason,
        operator_fixes: Vec::new(),
    }
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

    let schema = if errors.is_empty() {
        Some(Schema::new_with_metadata(
            output_fields,
            constraint.metadata().clone(),
        ))
    } else {
        None
    };

    Ok(SchemaReconciliationReport {
        schema,
        plan: SchemaCoercionPlan { fields: decisions },
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
    if is_lossless_widening(observed, constraint) {
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

fn is_lossless_widening(observed: &DataType, constraint: &DataType) -> bool {
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
