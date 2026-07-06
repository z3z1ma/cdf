use arrow_schema::Field;
use firn_kernel::{FirnError, Result, TypeMapping, TypeMappingFidelity, semantic};
use serde::{Deserialize, Serialize};

use crate::{
    normalization::{normalize_identifier, normalize_schema, validate_normalizer},
    policy::*,
    program::*,
    schema::*,
    transforms::TransformColumn,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypeMappingDecision {
    AllowedLossless,
    AllowedLossyByContract,
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
